import csv
import dataclasses
import json
import logging
import os
import signal
import tempfile
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


class ClientProgress:
    """Estado durable del cliente, persistido con escritura atomica
    (temp -> fsync -> os.replace).

    El envio de DATOS ya no se reanuda desde aca: el cliente streamea sin ACK por
    batch y la autoridad de reanudacion es el gateway (le dice desde que msg_id
    retomar en el hello_ack). Por eso lo unico que se persiste es:

    - uuid: identidad durable del cliente (para presentarse al reconectar).
    - pending_eofs: EOF de resultado que faltan (reanuda la fase de RESULTADOS
      tras una caida del cliente sin re-esperar los ya contados).
    - received_results: estado de dedup last_seen-por-sender (no reescribir filas
      de resultado ya recibidas ante reentregas)."""

    def __init__(self, path):
        self.path = path
        self.uuid = None
        self.pending_eofs = _EXPECTED_RESULTS
        self.received_results = {}

    @classmethod
    def load(cls, path):
        p = cls(path)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    d = json.load(f)
                p.uuid = d.get("uuid")
                p.pending_eofs = d.get("pending_eofs", _EXPECTED_RESULTS)
                p.received_results = d.get("received_results", {})
                logging.info("Loaded progress: %s", d)
            except Exception as e:
                logging.error("Corrupted progress file %s: %s", path, e)
        return p

    def save(self):
        d = {
            "uuid": self.uuid,
            "pending_eofs": self.pending_eofs,
            "received_results": self.received_results,
        }
        directory = os.path.dirname(self.path) or "."
        fd, temp_path = tempfile.mkstemp(dir=directory, prefix="tmp_progress_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(d, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.path)
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def set_uuid(self, uuid):
        self.uuid = uuid
        self.save()

    def set_pending_eofs(self, pending_eofs):
        self.pending_eofs = pending_eofs
        self.save()

    def set_received_results(self, dedup_state):
        self.received_results = dedup_state
        self.save()


class Client:
    """Cliente con identidad durable y envio en streaming hacia el gateway.

    Handshake: al conectar manda `hello` (con su UUID si lo tiene, o vacio para
    que el gateway le asigne uno) y espera `hello_ack`, que ademas trae
    `resume_from`: el ultimo msg_id que el gateway reenvio downstream de forma
    durable. El cliente streamea desde resume_from + 1.

    Envio de datos: streamea accounts + transactions con un msg_id monotonico SIN
    esperar ACK por batch. Si la conexion se cae, reconecta (el handshake le
    devuelve el nuevo punto de reanudacion) y re-streamea salteando lo ya
    reenviado. Los workers deduplican el solape por (client, sender, msg_id).

    EOF: stop-and-wait (barrera). Resultados: ver _recv_results."""

    def __init__(self, server_host, server_port, output_path):
        self.server_host = server_host
        self.server_port = server_port
        self.output_path = output_path
        self.closed = False

        self._progress = ClientProgress.load(
            os.path.join(output_path, ".client_progress.json")
        )
        self.client_id = self._progress.uuid
        # Cursor de reanudacion de datos: ultimo msg_id que el gateway ya tiene.
        # Lo fija el hello_ack (resume_from); -1 = empezar de cero.
        self._cursor = -1
        # msg_id del EOF (= total de batches), calculado al terminar el stream.
        self._eof_msg_id = None
        # pending_eofs es durable: si el cliente se cae recibiendo resultados,
        # al reanudar no vuelve a esperar los EOF que ya conto.
        self._pending_eofs = self._progress.pending_eofs
        # Dedup de resultados por (sender, msg_id): evita reescribir filas en los
        # CSV ante reentregas de RabbitMQ (p.ej. tras un restart del gateway).
        self._dedup = DuplicateHandler()

        self.sock = _connect_with_retry(server_host, server_port)
        signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.closed = True
        try:
            self.sock.shutdown("rdwr")
        except OSError:
            pass

    # ----- handshake / conexion -----

    def _handshake(self):
        """Manda `hello` y espera `hello_ack`, fijando self.client_id. Reintenta
        reconectando ante fallos de socket."""
        while not self.closed:
            try:
                self.sock.send_bytes(
                    serialize(
                        build_hello_message(uuid=self._progress.uuid, sender=_SENDER)
                    )
                )
                ack = self._recv_until_type("hello_ack")
                if ack is None:
                    return
                assigned = ack.get("client")
                if self._progress.uuid is None:
                    self._progress.set_uuid(assigned)  # persiste el UUID nuevo
                self.client_id = assigned
                # Punto de reanudacion de datos que dicta el gateway.
                self._cursor = ack.get("resume_from", -1)
                logging.info(
                    "Handshake done; client_id=%s resume_from=%d",
                    self.client_id,
                    self._cursor,
                )
                return
            except (ConnectionError, OSError) as exc:
                logging.warning("Handshake failed: %s; reconnecting", exc)
                self._reconnect_socket()

    def _reconnect_socket(self):
        try:
            self.sock.close()
        except OSError:
            pass
        self.sock = _connect_with_retry(self.server_host, self.server_port)

    def _reconnect(self):
        """Reconecta y rehace el handshake antes de seguir enviando datos."""
        self._reconnect_socket()
        self._handshake()
        logging.info("Reconnected and re-handshaked (client_id=%s)", self.client_id)

    # ----- envio de datos (streaming) -----

    def start(self, accounts_path, transactions_path, batch_size):
        self._handshake()
        # Ya tenemos client_id definitivo: restauramos el estado de dedup de
        # resultados persistido (vacio en una corrida nueva).
        self._dedup.restore_state(self.client_id, self._progress.received_results)
        logging.info(
            "Streaming data (client_id=%s, resume_from=%d)",
            self.client_id,
            self._cursor,
        )

        self._eof_msg_id = self._stream_data(
            accounts_path, transactions_path, batch_size
        )
        if self.closed:
            return

        # El EOF es stop-and-wait. Si el gateway ya lo tenia (cursor al dia con el
        # EOF), saltamos directo a resultados.
        if self._cursor < self._eof_msg_id:
            logging.info("Data streamed; sending EOF (msg_id=%d)", self._eof_msg_id)
            self._send_eof_reliable(self._eof_msg_id)
        else:
            logging.info(
                "EOF already acknowledged (cursor=%d); skipping", self._cursor
            )
        if self.closed:
            return

        logging.info("Datasets sent; waiting for results")
        self._recv_results()
        self.sock.close()

    def _stream_data(self, accounts_path, transactions_path, batch_size):
        """Streamea accounts y luego transactions sin esperar ACK por batch.
        Ante una caida reconecta (el handshake actualiza self._cursor con el nuevo
        resume_from) y re-streamea desde el inicio salteando lo ya reenviado.
        Devuelve el msg_id que le corresponde al EOF (= total de batches)."""
        while not self.closed:
            try:
                return self._send_all_batches(
                    accounts_path, transactions_path, batch_size
                )
            except (ConnectionError, OSError) as exc:
                logging.warning(
                    "Connection lost streaming data (cursor=%d): %s; reconnecting",
                    self._cursor,
                    exc,
                )
                self._reconnect()
        return self._cursor + 1

    def _send_all_batches(self, accounts_path, transactions_path, batch_size):
        """Asigna un msg_id monotonico desde 0 a cada batch (accounts y luego
        transactions) y envia solo los > self._cursor (los <= ya los tiene el
        gateway). Devuelve el total de batches = msg_id del EOF."""
        msg_id = 0
        streams = (
            (STREAM_ACCOUNTS, accounts_path),
            (STREAM_TRANSACTIONS, transactions_path),
        )
        for stream, path in streams:
            for batch in read_csv_batches(path, batch_size, stream):
                if msg_id > self._cursor:
                    self.sock.send_bytes(
                        serialize(
                            build_stream_message(
                                stream,
                                client=self.client_id,
                                msg_id=msg_id,
                                batch=batch,
                                sender=_SENDER,
                            )
                        )
                    )
                msg_id += 1
                if self.closed:
                    return msg_id
        return msg_id

    def _send_eof_reliable(self, msg_id):
        """Envia el EOF (stop-and-wait) y bloquea hasta su ACK. Ante una caida
        reconecta y reenvia el mismo msg_id; es idempotente (el gateway re-forwarda
        deduplicado y re-persiste el cursor)."""
        payload = serialize(
            build_eof_message(client=self.client_id, msg_id=msg_id, sender=_SENDER)
        )
        while not self.closed:
            try:
                self.sock.send_bytes(payload)
                self._wait_for_ack(msg_id)
                return
            except (ConnectionError, OSError) as exc:
                logging.warning(
                    "Connection lost waiting EOF ack (msg_id %d): %s; reconnecting",
                    msg_id,
                    exc,
                )
                self._reconnect()

    def _wait_for_ack(self, msg_id):
        """Recibe hasta el ACK de `msg_id`. True con el ACK correcto, False si el
        cliente se cierra. Propaga ConnectionError si el socket se cae."""
        while not self.closed:
            decoded = deserialize(self.sock.recv_bytes())
            if decoded.get("type") == "ack":
                if decoded.get("msg_id") == msg_id:
                    return True
                logging.debug(
                    "Ignoring stale ACK %s (waiting %d)", decoded.get("msg_id"), msg_id
                )
                continue
            # No deberian llegar resultados durante la ingesta, pero si llegan
            # los persistimos para no perderlos.
            if self._persist_message(decoded):
                self._count_eof()
        return False

    def _recv_until_type(self, msg_type):
        """Recibe hasta un mensaje de tipo `msg_type` (handshake). Los resultados
        que aparezcan en el camino se persisten (no se descartan): al reconectar
        en la fase de resultados, el egress del gateway puede reenviar un
        resultado en la ventana entre el registro de la sesion y el hello_ack."""
        while not self.closed:
            decoded = deserialize(self.sock.recv_bytes())
            if decoded.get("type") == msg_type:
                return decoded
            if self._persist_message(decoded):
                self._count_eof()
        return None

    def _recv_results(self):
        logging.info("Receiving results")
        while self._pending_eofs > 0 and not self.closed:
            try:
                msg = self.sock.recv_bytes()
            except (ConnectionError, OSError) as exc:
                if self.closed:
                    return
                # El gateway se cayo mientras recibiamos resultados: reconectamos
                # (esperando a que vuelva) y le reenviamos nuestro EOF para
                # re-señalar que ya terminamos de enviar y seguimos esperando
                # resultados. Proseguimos donde quedamos (pending_eofs es durable).
                logging.warning(
                    "Connection lost receiving results (%d pending): %s; reconnecting",
                    self._pending_eofs,
                    exc,
                )
                self._reconnect()
                self._resend_eof()
                continue
            decoded = deserialize(msg)
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
                self._progress.set_pending_eofs(0)
                return
            if self._persist_message(decoded):
                self._count_eof()

    def _resend_eof(self):
        """Reenvia el EOF de entrada con su msg_id original tras reconectar en la
        fase de resultados. Idempotente: los workers deduplican por
        (gateway, msg_id) y el gateway por su progreso durable de resultados. Si
        el socket vuelve a caerse, el proximo recv del loop lo detecta y reconecta."""
        if self._eof_msg_id is None:
            return
        try:
            self.sock.send_bytes(
                serialize(
                    build_eof_message(
                        client=self.client_id,
                        msg_id=self._eof_msg_id,
                        sender=_SENDER,
                    )
                )
            )
        except (ConnectionError, OSError):
            pass

    def _count_eof(self):
        """Descuenta un EOF de resultado ya recibido y persiste el progreso."""
        self._pending_eofs -= 1
        self._progress.set_pending_eofs(self._pending_eofs)

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
        if self._dedup.is_duplicate(self.client_id, sender, msg_id):
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

        self._dedup.mark_seen(self.client_id, sender, msg_id)
        self._progress.set_received_results(self._dedup.get_state(self.client_id))
        return is_eof


def main() -> int:
    execution_time = time.time()
    logging.basicConfig(level=logging.INFO)
    # Solo truncamos los CSV de salida en una corrida nueva. Si hay progreso
    # persistido estamos reanudando: truncar borraria resultados ya recibidos y
    # contradiria el estado de dedup, que asume que esas filas ya estan escritas.
    resuming = os.path.exists(os.path.join(OUTPUT_PATH, ".client_progress.json"))
    if not resuming:
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
