import dataclasses
import logging
import csv
import os
import time
import threading
import signal

from src.common.communication import (
    STREAM_ACCOUNTS,
    STREAM_TRANSACTIONS,
    connect,
    send_csv,
    send_eof,
    deserialize,
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


_opened_files: set = set()


def persist_rows(output_file, batch):
    mode = "w" if output_file not in _opened_files else "a"
    _opened_files.add(output_file)
    with open(output_file, mode) as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=",", quotechar='"')
        for row in batch:
            row_dict = dataclasses.asdict(row) if dataclasses.is_dataclass(row) else row
            
            csv_writer.writerow(
                v for v in row_dict.values() if v is not None
            )


class Client:
    def __init__(self, server_host, server_port, output_path):
        self.closed = False
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)
        self.server_socket = _connect_with_retry(server_host, server_port)
        self._reader_thread = threading.Thread(
            target=self.recv_results,
            args=(
                self.server_socket,
                output_path,
            ),
            daemon=True,  # si el proceso muere, el thread no bloquea el exit
        )
        self._reader_thread.start()

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.closed = True
        self.disconnect()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def disconnect(self):
        if self.server_socket:
            # shutdown() es mejor que close(): señaliza al peer sin cerrar
            # el fd inmediatamente, dando tiempo al thread lector a drenar.
            # HOW: SHUT_WR señaliza EOF al server; el thread lector recibe
            # EOF de vuelta y termina; luego close() libera el fd.
            self.server_socket.shutdown("rdwr")
            self.server_socket.close()

        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=5)

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
            # Con o sin error, cerramos escritura: el server sabrá que no
            # hay más datos y eventualmente cerrará su lado → el thread lector
            # recibirá EOF y terminará naturalmente
            self.server_socket.shutdown("wr")

        # Esperamos a que el thread lector termine limpiamente
        self._reader_thread.join()
        self.server_socket.close()

    def recv_results(self, sock, output_path):
        logging.info("Receiving results")
        query_result_counter = 0
        while query_result_counter < 5:
            try:
                msg = sock.recv_bytes()
            except ConnectionError:
                logging.info(
                    "Server closed connection (received %d result message(s))",
                    query_result_counter,
                )
                break

            decoded = deserialize(msg)
            query_type = decoded["type"]

            if query_type == "q1_result":
                output_file = output_path + "q1.csv"
                batch = decoded["payload"]["batch"]
                if decoded["eof"]:
                    query_result_counter += 1
                else:
                    persist_rows(output_file, batch)

            elif query_type == "q2_result":
                output_file = output_path + "q2.csv"
                batch = decoded["payload"]["batch"]
                persist_rows(output_file, batch)

            elif query_type == "q3_result":
                output_file = output_path + "q3.csv"
                batch = decoded["payload"]["batch"]
                persist_rows(output_file, batch)

            elif query_type == "q4_result":
                output_file = output_path + "q4.csv"
                batch = decoded["payload"]["batch"]
                persist_rows(output_file, batch)

            elif query_type == "q5_result":
                output_file = output_path + "q5.csv"
                batch = decoded["payload"]["batch"]
                persist_rows(output_file, batch)

            else:
                logging.info("Unexpected Message: %s", decoded)
                continue

        sock.shutdown("rd")
        sock.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client(SERVER_HOST, SERVER_PORT, OUTPUT_PATH)
    client.start(ACCOUNTS_PATH, TRANSACTIONS_PATH, BATCH_SIZE)

    return 0


if __name__ == "__main__":
    main()
