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
    connect,
    deserialize,
    send_csv,
    send_eof,
)

_CONNECT_RETRY_DELAY = 1.0
_CONNECT_MAX_RETRIES = 30
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
ACCOUNTS_PATH = os.environ["ACCOUNTS_PATH"]
TRANSACTIONS_PATH = os.environ["TRANSACTIONS_PATH"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
OUTPUT_PATH = os.environ["OUTPUT_PATH"]


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
_EXPECTED_RESULTS = len(_RESULT_FILES)  # un EOF por query


class Client:
    def __init__(self, server_host, server_port, output_path):
        self.output_path = output_path
        self.closed = False
        self.server_socket = _connect_with_retry(server_host, server_port)
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        # El lector SOLO lee y persiste; nunca toca el ciclo de vida del socket.
        # El dueño del socket es el thread principal: lo crea y lo cierra.
        self._reader_thread = threading.Thread(
            target=self.recv_results,
            daemon=True,  # si el proceso muere, el thread no bloquea el exit
        )
        self._reader_thread.start()

    def handle_sigterm(self, signum, frame):
        # Corre en el thread principal. NO cierra ni hace join: solo marca el
        # cierre y despierta al lector bloqueado en recv() cerrando el socket.
        # El thread principal sigue en start() y hace el cierre ordenado, asi
        # nunca hay dos threads tocando el socket a la vez.
        logging.info("Received SIGTERM signal")
        self.closed = True
        try:
            self.server_socket.shutdown("rdwr")
        except OSError:
            pass

    def start(self, accounts_path, transactions_path, batch_size):
        logging.info("Sending data")
        try:
            send_csv(self.server_socket, accounts_path, batch_size, STREAM_ACCOUNTS)
            send_csv(
                self.server_socket, transactions_path, batch_size, STREAM_TRANSACTIONS
            )
            send_eof(self.server_socket)
            logging.info("Datasets sent; waiting for results")
        finally:
            # Cerramos solo la escritura: el server sabe que no hay mas datos,
            # pero seguimos leyendo resultados. Puede fallar si un SIGTERM ya
            # hizo shutdown del socket; en ese caso no hay nada que señalizar.
            try:
                self.server_socket.shutdown("wr")
            except OSError:
                pass

        # El lector termina al juntar los EOFs (o si el server corta). Recien
        # cuando termino de persistir cerramos el fd: UN solo close, UN solo thread.
        self._reader_thread.join()
        self.server_socket.close()

    def recv_results(self):
        logging.info("Receiving results")
        pending_eofs = _EXPECTED_RESULTS
        # 'self.closed' lo escribe el handler (mismo proceso, GIL mediante) y lo
        # lee este thread: alcanza para cortar el loop si el shutdown nos desperto.
        while pending_eofs > 0 and not self.closed:
            try:
                msg = self.server_socket.recv_bytes()
            except ConnectionError:
                logging.info(
                    "Server closed connection with %d EOF(s) still pending",
                    pending_eofs,
                )
                return

            if self._persist_message(deserialize(msg)):
                pending_eofs -= 1

    def _persist_message(self, decoded):
        """Persiste un batch. Devuelve True solo si era el EOF final de una query."""
        file_name = _RESULT_FILES.get(decoded["type"])
        if file_name is None:
            logging.info("Unexpected Message: %s", decoded)
            return False
        batch = decoded["payload"]["batch"]
        if decoded["eof"]:
            logging.info("Query result EOF: %s", decoded)
            if len(batch) > 0:
                persist_rows(self.output_path + file_name, batch)
            # El batch del EOF viene vacio: no se persiste, solo cuenta el cierre.
            return True
        persist_rows(self.output_path + file_name, batch)
        return False


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    for i in range(1, 6):
        # Create 5 output files
        file_name = f"q{i}.csv"
        output_file = os.path.join(OUTPUT_PATH, file_name)
        file = open(output_file, "w")
        file.close()

    client = Client(SERVER_HOST, SERVER_PORT, OUTPUT_PATH)
    client.start(ACCOUNTS_PATH, TRANSACTIONS_PATH, BATCH_SIZE)

    return 0


if __name__ == "__main__":
    main()
