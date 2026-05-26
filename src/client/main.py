import logging
import os

from src.common.communication import connect, send_csv


def run_client(host, port, dataset_path, batch_size):
    sock = connect(host, port)
    try:
        send_csv(sock, dataset_path, batch_size)
    finally:
        sock.close()


def main():
    logging.basicConfig(level=logging.INFO)

    host = os.environ["SERVER_HOST"]
    port = int(os.environ["SERVER_PORT"])
    dataset_path = os.environ["DATASET_PATH"]
    batch_size = int(os.environ.get("BATCH_SIZE", "500"))

    logging.info(
        "Client starting: host=%s port=%s dataset=%s batch_size=%s",
        host,
        port,
        dataset_path,
        batch_size,
    )
    run_client(host, port, dataset_path, batch_size)
    logging.info("Client done")


if __name__ == "__main__":
    main()
