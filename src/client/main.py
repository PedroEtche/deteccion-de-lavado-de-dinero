import csv
import dataclasses
import logging
import os
import signal
import threading
import time

from src.common.communication import (
    STREAM_ACCOUNTS,
    STREAM_TRANSACTIONS,
    build_eof_message,
    build_hello_message,
    build_stream_message,
    connect,
    deserialize,
    read_csv_batches,
    serialize,
)
from src.common.duplicate_handler import DuplicateHandler

_CONNECT_RETRY_DELAY = 1.0
_CONNECT_MAX_RETRIES = 30
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
ACCOUNTS_PATH = os.environ["ACCOUNTS_PATH"]
TRANSACTIONS_PATH = os.environ["TRANSACTIONS_PATH"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
OUTPUT_PATH = os.environ["OUTPUT_PATH"]

_SENDER = "client"


def _connect_with_retry(host, port):
    for attempt in range(1, _CONNECT_MAX_RETRIES + 1):
        try:
            return connect(host, port)
        except (ConnectionRefusedError, OSError) as exc:
            if attempt == _CONNECT_MAX_RETRIES:
                raise
            logging.info(
                "Connection to %s:%s refused (attempt %d/%d): %s. Retrying in %ss",
                host,
                port,
                attempt,
                _CONNECT_MAX_RETRIES,
                exc,
                _CONNECT_RETRY_DELAY,
            )
            time.sleep(_CONNECT_RETRY_DELAY)


def persist_rows(output_file, batch):
    with open(output_file, "a") as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
        for row in batch:
            row_dict = dataclasses.asdict(row) if dataclasses.is_dataclass(row) else row

            csv_writer.writerow(v for v in row_dict.values() if v is not None)


# Tabla tipo-de-resultado -> archivo: evita las 5 ramas repetidas del if/elif
# y hace que agregar/quitar una query sea cambiar una sola linea.
_RESULT_FILES = {
    "q1_result": "q1.csv",
    "q2_result": "q2.csv",
    "q3_result": "q3.csv",
    "q4_result": "q4.csv",
    "q5_result": "q5.csv",
}
_EXPECTED_RESULTS = len(_RESULT_FILES)  # un EOF por query (5 queries)


class GatewayConnection:
    """Conexion al gateway compartida por el lector y el escritor.

    Los dos threads usan el mismo socket a la vez: uno solo lee y el otro solo
    escribe, lo cual es seguro. El problema es la reconexion: si el gateway se
    cae, las dos operaciones fallan casi al mismo tiempo y los dos threads
    intentarian reconectar. Para que reconecte UNO solo usamos un lock y un
    contador de 'generacion': cada socket nuevo tiene una generacion mayor.

    Cuando una operacion falla, el thread llama a recover() pasando la generacion
    del socket que estaba usando. Si esa generacion sigue siendo la actual, es el
    primero en enterarse y reconecta de verdad (bajo el lock). Si ya avanzo, otro
    thread reconecto mientras este esperaba el lock, asi que no hace nada y toma
    el socket nuevo.

    El handshake (hello/hello_ack) tambien corre bajo el lock dentro de recover(),
    asi nunca hay dos recv() a la vez sobre el mismo socket: el thread que no
    reconecta esta bloqueado en el lock o todavia en su recv() del socket viejo,
    que al cerrarse lo despierta con error."""

    def __init__(self, host, port, sender):
        self._host = host
        self._port = port
        self._sender = sender
        self._lock = threading.Lock()
        self._generation = 0
        self._sock = None
        self.client_id = None
        self.resume_from = -1
        self.closed = False

    def connect(self):
        """Primera conexion + handshake. Corre en el thread principal antes de
        arrancar el lector."""
        self._sock = _connect_with_retry(self._host, self._port)
        self._handshake()

    def generation(self):
        return self._generation

    def send_message(self, message):
        """Serializa y envia. Puede lanzar ConnectionError/OSError si el socket
        se cayo; el que llama reacciona con recover()."""
        self._sock.send_bytes(serialize(message))

    def recv_message(self):
        """Recibe y deserializa. Puede lanzar ConnectionError/OSError; el que
        llama reacciona con recover()."""
        return deserialize(self._sock.recv_bytes())

    def recover(self, seen_generation):
        """Garantiza un socket vivo tras una falla. Reconecta solo si nadie lo
        hizo ya (la generacion no cambio desde que el que llama empezo a usar el
        socket). El otro thread, mientras tanto, espera en el lock."""
        with self._lock:
            if self.closed or seen_generation != self._generation:
                # Otro thread ya reconecto (o nos estamos cerrando): el socket
                # actual ya es el bueno, no hay nada que hacer.
                return
            self._reconnect_socket()
            self._handshake()
            self._generation += 1
            logging.info(
                "Reconnected (generation=%d, client_id=%s, resume_from=%d)",
                self._generation,
                self.client_id,
                self.resume_from,
            )

    def shutdown(self):
        """Despierta a los threads bloqueados en recv()/send() cerrando el
        socket. Lo usa el handler de SIGTERM."""
        self.closed = True
        try:
            self._sock.shutdown("rdwr")
        except OSError:
            pass

    def close(self):
        try:
            self._sock.close()
        except OSError:
            pass

    # ----- helpers internos -----

    def _reconnect_socket(self):
        try:
            self._sock.close()
        except OSError:
            pass
        self._sock = _connect_with_retry(self._host, self._port)

    def _handshake(self):
        """Manda `hello` (con nuestro client_id si ya lo tenemos, o vacio para
        que el gateway nos asigne uno) y espera `hello_ack`, que confirma el
        client_id y trae `resume_from`: el ultimo msg_id que el gateway reenvio
        downstream de forma durable. Streameamos desde resume_from + 1."""
        self._sock.send_bytes(
            serialize(build_hello_message(uuid=self.client_id, sender=self._sender))
        )
        ack = deserialize(self._sock.recv_bytes())
        self.client_id = ack.get("client")
        self.resume_from = ack.get("resume_from", -1)
        logging.info(
            "Handshake done; client_id=%s resume_from=%d",
            self.client_id,
            self.resume_from,
        )


class Client:
    """Cliente con dos threads sobre una unica conexion al gateway:

    - El thread principal (escritor) streamea accounts + transactions y al final
      manda el EOF. Si la conexion se cae, reconecta y re-streamea salteando lo
      que el gateway ya tiene (resume_from). Los workers deduplican.
    - Un thread lector recibe los resultados, los persiste y cuenta sus EOF.

    Si el gateway se cae, lo detecta cualquiera de los dos threads y reconecta
    UNO solo (ver GatewayConnection). Si el cliente muere, arranca de cero: no
    persiste progreso. El dedup de resultados es solo en memoria, para no
    reescribir filas si el gateway reenvia resultados tras su propio restart."""

    def __init__(self, server_host, server_port, output_path):
        self.output_path = output_path
        self.closed = False

        self.conn = GatewayConnection(server_host, server_port, _SENDER)

        # msg_id del EOF de entrada (= total de batches). Lo fija el escritor al
        # terminar de streamear; el lector lo usa para reenviar el EOF tras
        # reconectar en la fase de resultados.
        self._eof_msg_id = None
        self._pending_eofs = _EXPECTED_RESULTS

        # Dedup de resultados por (sender, msg_id): evita reescribir filas en los
        # CSV ante una reentrega (p.ej. tras un restart del gateway).
        self._dedup = DuplicateHandler()

        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.closed = True
        self.conn.shutdown()

    def start(self, accounts_path, transactions_path, batch_size):
        # 1) Handshake inicial en el thread principal.
        self.conn.connect()

        # 2) Arrancamos el lector lo antes posible: antes de empezar a streamear,
        #    asi nunca quedan resultados sin leer.
        reader_thread = threading.Thread(target=self._recv_results, daemon=True)
        reader_thread.start()

        # 3) El propio thread principal hace de escritor.
        self._send_loop(accounts_path, transactions_path, batch_size)

        if self.closed:
            return

        logging.info("Datasets sent; waiting for results")
        reader_thread.join()
        self.conn.close()

    # ----- escritor -----

    def _send_loop(self, accounts_path, transactions_path, batch_size):
        """Streamea todos los batches y manda el EOF. Ante una caida reconecta
        (el handshake actualiza resume_from) y reintenta desde el principio
        salteando lo ya enviado."""
        while not self.closed:
            seen_generation = self.conn.generation()
            try:
                eof_msg_id = self._stream_all_batches(
                    accounts_path, transactions_path, batch_size
                )
                if self.closed:
                    return
                self._eof_msg_id = eof_msg_id
                logging.info("Data streamed; sending EOF (msg_id=%d)", eof_msg_id)
                self.conn.send_message(
                    build_eof_message(
                        client=self.conn.client_id,
                        msg_id=eof_msg_id,
                        sender=_SENDER,
                    )
                )
                return
            except (ConnectionError, OSError) as exc:
                logging.warning("Connection lost while sending: %s; reconnecting", exc)
                self.conn.recover(seen_generation)

    def _stream_all_batches(self, accounts_path, transactions_path, batch_size):
        """Asigna un msg_id monotonico desde 0 a cada batch (accounts y luego
        transactions) y envia solo los > resume_from (los <= ya los tiene el
        gateway). Devuelve el total de batches = msg_id del EOF."""
        msg_id = 0
        streams = (
            (STREAM_ACCOUNTS, accounts_path),
            (STREAM_TRANSACTIONS, transactions_path),
        )
        for stream, path in streams:
            for batch in read_csv_batches(path, batch_size, stream):
                if msg_id > self.conn.resume_from:
                    self.conn.send_message(
                        build_stream_message(
                            stream,
                            client=self.conn.client_id,
                            msg_id=msg_id,
                            batch=batch,
                            sender=_SENDER,
                        )
                    )
                msg_id += 1
                if self.closed:
                    return msg_id
        return msg_id

    # ----- lector -----

    def _recv_results(self):
        logging.info("Receiving results")
        while self._pending_eofs > 0 and not self.closed:
            seen_generation = self.conn.generation()
            try:
                decoded = self.conn.recv_message()
            except (ConnectionError, OSError) as exc:
                if self.closed:
                    return
                # El gateway se cayo mientras recibiamos resultados: reconectamos
                # (esperando a que vuelva) y le reenviamos el EOF para reseñalar
                # que ya terminamos de enviar. Seguimos donde quedamos.
                logging.warning(
                    "Connection lost receiving results (%d pending): %s; reconnecting",
                    self._pending_eofs,
                    exc,
                )
                self.conn.recover(seen_generation)
                self._resend_eof()
                continue

            msg_type = decoded.get("type")
            if msg_type in ("ack", "hello_ack"):
                continue
            if msg_type == "results_done":
                # El gateway confirma que la sesion quedo completa. Cortamos aca
                # aunque queden EOF pendientes en nuestro contador: el gateway es
                # la autoridad de completitud (sus EOF de resultado son durables).
                logging.info(
                    "Gateway signaled results complete (%d pending locally)",
                    self._pending_eofs,
                )
                self._pending_eofs = 0
                return
            if self._persist_message(decoded):
                self._pending_eofs -= 1

    def _resend_eof(self):
        """Reenvia el EOF de entrada con su msg_id original tras reconectar en la
        fase de resultados. Idempotente: el gateway deduplica por su progreso
        durable. Si el socket vuelve a caerse, el proximo recv del loop lo detecta
        y reconecta."""
        if self._eof_msg_id is None:
            return
        try:
            self.conn.send_message(
                build_eof_message(
                    client=self.conn.client_id,
                    msg_id=self._eof_msg_id,
                    sender=_SENDER,
                )
            )
        except (ConnectionError, OSError):
            pass

    def _persist_message(self, decoded):
        """Persiste un batch. Devuelve True solo si era el EOF final (no visto
        aun) de una query. Deduplica por (sender, msg_id): una reentrega de
        RabbitMQ (p.ej. tras un restart del gateway) no reescribe filas ni vuelve
        a contar un EOF ya contado."""
        file_name = _RESULT_FILES.get(decoded["type"])
        if file_name is None:
            logging.info("Unexpected Message: %s", decoded)
            return False

        sender = decoded.get("sender")
        msg_id = decoded.get("msg_id")
        if self._dedup.is_duplicate(self.conn.client_id, sender, msg_id):
            logging.debug(
                "Duplicate result from %s msg_id %s; skipping", sender, msg_id
            )
            return False

        batch = decoded["payload"]["batch"]
        is_eof = bool(decoded["eof"])
        if is_eof:
            logging.info("Query result EOF: %s", decoded)
            if len(batch) > 0:
                persist_rows(self.output_path + file_name, batch)
        else:
            persist_rows(self.output_path + file_name, batch)

        self._dedup.mark_seen(self.conn.client_id, sender, msg_id)
        return is_eof


def main() -> int:
    execution_time = time.time()
    logging.basicConfig(level=logging.INFO)
    for i in range(1, 6):
        # Create 5 output files
        file_name = f"q{i}.csv"
        output_file = os.path.join(OUTPUT_PATH, file_name)
        file = open(output_file, "w")
        file.close()

    client = Client(SERVER_HOST, SERVER_PORT, OUTPUT_PATH)
    client.start(ACCOUNTS_PATH, TRANSACTIONS_PATH, BATCH_SIZE)

    execution_time = time.time() - execution_time
    minutes, seconds = divmod(execution_time, 60)
    logging.info("Client FINISH in: %d minutes and %f seconds", minutes, seconds)
    return 0


if __name__ == "__main__":
    main()
