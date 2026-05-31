import logging
import os
import signal
import socket
import threading
import uuid
from dataclasses import dataclass

import yaml

from src.common.communication import (
    STREAM_ACCOUNTS,
    STREAM_TRANSACTIONS,
    receive_streams,
)
from src.common.communication.tcp import TCPSocket
from src.common.middleware import MessageMiddlewareQueueRabbitMQ
from common.communication.internal import (
    AccountRow,
    TransactionRow,
    build_eof_message,
    build_raw_accounts_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)


CONFIG_PATH = "./config.yaml"


@dataclass
class GatewayConfig:
    host: str
    port: int
    mom_host: str
    transactions_queue: str
    accounts_queue: str
    result_queue: str
    log_level: str


def _load_file_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def init_config():
    file_config = _load_file_config()
    return GatewayConfig(
        host=os.getenv("SERVER_HOST", file_config.get("host", "0.0.0.0")),
        port=int(os.getenv("SERVER_PORT", file_config.get("port", 5678))),
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        transactions_queue=os.getenv(
            "TRANSACTIONS_QUEUE",
            file_config.get("transactions_queue", "transactions_queue"),
        ),
        accounts_queue=os.getenv(
            "ACCOUNTS_QUEUE",
            file_config.get("accounts_queue", "accounts_queue"),
        ),
        result_queue=os.getenv("INPUT_QUEUE", file_config.get("result_queue", "result_queue")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
    )


def log_config(config):
    logging.info(
        "Gateway startup with: host=%s | port=%s | mom_host=%s | transactions_queue=%s | accounts_queue=%s | result_queue=%s",
        config.host,
        config.port,
        config.mom_host,
        config.transactions_queue,
        config.accounts_queue,
        config.result_queue,
    )


_CSV_TO_TX_FIELDS = (
    ("Timestamp", "timestamp"),
    ("From Bank", "from_bank"),
    ("Account", "from_account"),
    ("To Bank", "to_bank"),
    ("Account.1", "to_account"),
    ("Amount Received", "amount_received"),
    ("Receiving Currency", "receiving_currency"),
    ("Amount Paid", "amount_paid"),
    ("Payment Currency", "payment_currency"),
    ("Payment Format", "payment_format"),
)

_TX_FLOAT_FIELDS = {"amount_received", "amount_paid"}


def _dict_to_transaction_row(row):
    kwargs = {}
    for csv_field, tx_field in _CSV_TO_TX_FIELDS:
        value = row.get(csv_field)
        if value is None or value == "":
            continue
        if tx_field in _TX_FLOAT_FIELDS:
            kwargs[tx_field] = float(value)
        else:
            kwargs[tx_field] = value
    return TransactionRow(**kwargs)


_CSV_TO_ACCOUNT_FIELDS = (
    ("Bank Name", "bank_name"),
    ("Bank ID", "bank_id"),
    ("Account Number", "account_number"),
    ("Entity ID", "entity_id"),
    ("Entity Name", "entity_name"),
)


def _dict_to_account_row(row):
    kwargs = {}
    for csv_field, ac_field in _CSV_TO_ACCOUNT_FIELDS:
        value = row.get(csv_field)
        if value is None or value == "":
            continue
        kwargs[ac_field] = value
    return AccountRow(**kwargs)


class GatewayService:
    def __init__(self, config):
        self.host = config.host
        self.port = config.port
        self.mom_host = config.mom_host
        self.transactions_queue = config.transactions_queue
        self.accounts_queue = config.accounts_queue
        self.result_queue = config.result_queue
        self._tx_mw = None
        self._accounts_mw = None
        self._result_mw = None
        self._server_sock = None
        self._running = False
        self._send_lock = threading.Lock()
        self._registry_lock = threading.Lock()
        self._registry: dict = {}
        self._result_thread = None
        self._client_threads: list = []

    def start(self):
        logging.info("Starting gateway service")
        self._running = True
        self._tx_mw = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.transactions_queue)
        self._accounts_mw = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.accounts_queue)
        self._result_mw = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.result_queue)

        self._result_thread = threading.Thread(
            target=self._result_mw.start_consuming,
            args=(self._dispatch_result,),
            daemon=True,
        )
        self._result_thread.start()

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(8)
        logging.info("Gateway listening on %s:%s", self.host, self.port)

        try:
            while self._running:
                try:
                    client_sock, addr = self._server_sock.accept()
                except OSError:
                    break
                logging.info("Client connected from %s", addr)
                t = threading.Thread(
                    target=self._serve_one_client,
                    args=(client_sock,),
                    daemon=True,
                )
                t.start()
                self._client_threads.append(t)
        finally:
            self._close_resources()

    def _dispatch_result(self, message, ack, _nack):
        try:
            decoded = deserialize(message)
        except Exception:
            logging.exception("error decoding result message; discarding")
            ack()
            return

        client_id = decoded.get("client")
        with self._registry_lock:
            entry = self._registry.get(client_id)

        if entry is None:
            logging.warning("No client registered for id %s; discarding result", client_id)
            ack()
            return

        try:
            entry["tcp"].send_bytes(message)
        except Exception:
            logging.exception("error forwarding result to client %s", client_id)
            entry["done"].set()
            ack()
            return

        ack()
        if decoded.get("type") == "eof":
            logging.info("Received EOF on result queue for client %s", client_id)
            entry["done"].set()

    def _serve_one_client(self, sock):
        tcp = TCPSocket(sock)
        client_id = str(uuid.uuid4())
        done = threading.Event()

        with self._registry_lock:
            self._registry[client_id] = {"tcp": tcp, "done": done}
        logging.info("Registered client %s", client_id)

        try:
            for stream, batch in receive_streams(tcp):
                if not batch:
                    continue
                if stream == STREAM_TRANSACTIONS:
                    txs = [_dict_to_transaction_row(row) for row in batch]
                    msg = build_raw_transactions_message(
                        client=client_id,
                        msg_id=str(uuid.uuid4()),
                        batch=txs,
                    )
                    with self._send_lock:
                        self._tx_mw.send(serialize(msg))
                elif stream == STREAM_ACCOUNTS:
                    accounts = [_dict_to_account_row(row) for row in batch]
                    msg = build_raw_accounts_message(
                        client=client_id,
                        msg_id=str(uuid.uuid4()),
                        batch=accounts,
                    )
                    with self._send_lock:
                        self._accounts_mw.send(serialize(msg))
                else:
                    logging.warning("Unknown stream byte: %d", stream)
            logging.info("Inbound EOF received for client %s; forwarding EOF downstream", client_id)
            with self._send_lock:
                self._tx_mw.send(serialize(build_eof_message(client=client_id, msg_id=str(uuid.uuid4()))))
                self._accounts_mw.send(serialize(build_eof_message(client=client_id, msg_id=str(uuid.uuid4()))))
            done.wait()
            logging.info("Pipeline complete for client %s; closing client socket", client_id)
        except ConnectionError:
            logging.info("Client %s disconnected during inbound", client_id)
        finally:
            with self._registry_lock:
                self._registry.pop(client_id, None)
            tcp.close()

    def stop(self):
        logging.info("Stopping gateway service")
        self._running = False
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except Exception:
                logging.exception("error closing server socket")
        if self._result_mw is not None:
            try:
                self._result_mw.stop_consuming()
            except Exception:
                logging.exception("error stopping result consumer")

    def _close_resources(self):
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except Exception:
                pass
        for mw in (self._tx_mw, self._accounts_mw, self._result_mw):
            if mw is None:
                continue
            try:
                mw.close()
            except Exception:
                logging.exception("error closing middleware")


def main():
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    service = GatewayService(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    service.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
