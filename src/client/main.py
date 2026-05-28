import logging
import os
import time

from src.common.communication import (
    STREAM_ACCOUNTS,
    STREAM_TRANSACTIONS,
    connect,
    send_csv,
    send_eof,
)


_CONNECT_RETRY_DELAY = 1.0
_CONNECT_MAX_RETRIES = 30


def _connect_with_retry(host, port):
    for attempt in range(1, _CONNECT_MAX_RETRIES + 1):
        try:
            return connect(host, port)
        except (ConnectionRefusedError, OSError) as exc:
            if attempt == _CONNECT_MAX_RETRIES:
                raise
            logging.info(
                "Connection to %s:%s refused (attempt %d/%d): %s. Retrying in %ss",
                host, port, attempt, _CONNECT_MAX_RETRIES, exc, _CONNECT_RETRY_DELAY,
            )
            time.sleep(_CONNECT_RETRY_DELAY)


def run_client(host, port, accounts_path, transactions_path, batch_size):
    sock = _connect_with_retry(host, port)
    try:
        send_csv(sock, accounts_path, batch_size, STREAM_ACCOUNTS)
        send_csv(sock, transactions_path, batch_size, STREAM_TRANSACTIONS)
        send_eof(sock)
        logging.info("Datasets sent; waiting for results")
        result_count = 0
        while True:
            try:
                msg = sock.recv_bytes()
            except ConnectionError:
                logging.info("Server closed connection (received %d result message(s))", result_count)
                break
            result_count += 1
            logging.info("Result %d: %s", result_count, msg.decode("utf-8", errors="replace"))
    finally:
        sock.close()


def main():
    logging.basicConfig(level=logging.INFO)

    host = os.environ["SERVER_HOST"]
    port = int(os.environ["SERVER_PORT"])
    accounts_path = os.environ["ACCOUNTS_DATASET_PATH"]
    transactions_path = os.environ["TRANSACTIONS_DATASET_PATH"]
    batch_size = int(os.environ.get("BATCH_SIZE", "500"))

    logging.info(
        "Client starting: host=%s port=%s accounts=%s transactions=%s batch_size=%s",
        host,
        port,
        accounts_path,
        transactions_path,
        batch_size,
    )
    run_client(host, port, accounts_path, transactions_path, batch_size)
    logging.info("Client done")


if __name__ == "__main__":
    main()
