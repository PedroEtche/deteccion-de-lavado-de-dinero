import logging
import os
import signal
import socket
import threading
import uuid
from dataclasses import dataclass

import yaml

from src.common.communication.protocol import receive_batches
from src.common.communication.tcp import TCPSocket
from src.common.middleware import MessageMiddlewareQueueRabbitMQ
from src.communication.protocols.queue_protocol.internal import (
    TransactionRow,
    build_raw_transactions_message,
    serialize,
)


CONFIG_PATH = "./config.yaml"


@dataclass
class GatewayConfig:
    host: str
    port: int
    mom_host: str
    transactions_queue: str
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
        result_queue=os.getenv("INPUT_QUEUE", file_config.get("result_queue", "result_queue")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
    )


def log_config(config):
    logging.info(
        "Gateway startup with: host=%s | port=%s | mom_host=%s | transactions_queue=%s | result_queue=%s",
        config.host,
        config.port,
        config.mom_host,
        config.transactions_queue,
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

_FLOAT_FIELDS = {"amount_received", "amount_paid"}


def _dict_to_transaction_row(row):
    kwargs = {}
    for csv_field, tx_field in _CSV_TO_TX_FIELDS:
        value = row.get(csv_field)
        if value is None or value == "":
            continue
        if tx_field in _FLOAT_FIELDS:
            kwargs[tx_field] = float(value)
        else:
            kwargs[tx_field] = value
    return TransactionRow(**kwargs)


class GatewayService:
    # TODO multi-cliente: este gateway atiende un cliente por vez (loop serial
    # accept -> handle -> loop). Para soportar concurrencia con N clientes:
    #   1) Generar un client_id (uuid) por accept en lugar de hardcodear "client-0".
    #   2) Mantener un registry {client_id: TCPSocket} protegido por Lock.
    #   3) Spawn thread-per-connection en vez del handle serial.
    #   4) El result consumer enruta cada mensaje al socket correcto leyendo
    #      decoded["client"] del mensaje y buscando en el registry.
    # Importante: los workers downstream (filter/group/join) tienen que procesar
    # transacciones solo del cliente correspondiente. El protocolo interno ya
    # lleva el client_id en cada mensaje y process_message del filter lo preserva.
    # Pero cuando aparezcan aggregators con estado (Q2 max por banco, Q4 grupos
    # por cuenta), tienen que mantener ese estado *por client_id* — pipelines
    # de distintos clientes no deben mezclarse.

    def __init__(self, config):
        self.host = config.host
        self.port = config.port
        self.mom_host = config.mom_host
        self.transactions_queue = config.transactions_queue
        self.result_queue = config.result_queue
        self._input_mw = None
        self._server_sock = None
        self._running = False

    def start(self):
        logging.info("Starting gateway service")
        self._running = True
        self._input_mw = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.transactions_queue)

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        self._server_sock.listen(1)
        logging.info("Gateway listening on %s:%s", self.host, self.port)

        try:
            while self._running:
                try:
                    client_sock, addr = self._server_sock.accept()
                except OSError:
                    break
                logging.info("Client connected from %s", addr)
                self._serve_one_client(client_sock)
        finally:
            self._close_resources()

    def _serve_one_client(self, sock):
        tcp = TCPSocket(sock)
        output_mw = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.result_queue)
        forward_failed = threading.Event()

        def forward_to_client(message, ack, nack):
            try:
                tcp.send_bytes(message)
                ack()
            except Exception:
                logging.exception("error forwarding result (client probably closed the socket)")
                forward_failed.set()
                nack()

        consumer_thread = threading.Thread(
            target=output_mw.start_consuming,
            args=(forward_to_client,),
            daemon=True,
        )
        consumer_thread.start()

        try:
            for batch in receive_batches(tcp):
                txs = [_dict_to_transaction_row(row) for row in batch]
                if not txs:
                    continue
                msg = build_raw_transactions_message(
                    client="client-0",
                    msg_id=str(uuid.uuid4()),
                    batch=txs,
                )
                self._input_mw.send(serialize(msg))
            # Sin EOF propagation, no sabemos cuando termina el procesamiento.
            # Mantenemos el socket abierto hasta que el cliente cierre, momento
            # en el cual el forward_to_client falla y setea el event.
            logging.info("Inbound EOF received; waiting for client to close the socket")
            forward_failed.wait()
        except ConnectionError:
            logging.info("Client disconnected during inbound")
        finally:
            try:
                output_mw.stop_consuming()
            except Exception:
                logging.exception("error stopping result consumer")
            try:
                output_mw.close()
            except Exception:
                logging.exception("error closing output middleware")
            tcp.close()

    def stop(self):
        logging.info("Stopping gateway service")
        self._running = False
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except Exception:
                logging.exception("error closing server socket")

    def _close_resources(self):
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except Exception:
                pass
        if self._input_mw is not None:
            try:
                self._input_mw.close()
            except Exception:
                logging.exception("error closing input middleware")


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
