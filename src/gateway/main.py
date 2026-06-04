import logging
import os
import signal
import socket
import threading
import uuid
from dataclasses import dataclass

import yaml

from src.common.communication.tcp import TCPSocket
from src.common.middleware import MessageMiddlewareQueueRabbitMQ
from src.common.communication.internal import (
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
        result_queue=os.getenv(
            "RESULT_QUEUE",
            file_config.get("result_queue", "result_queue"),
        ),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
    )


def log_config(config: GatewayConfig):
    logging.info(
        "Gateway startup with: host=%s | port=%s | mom_host=%s | transactions_queue=%s | accounts_queue=%s | result_queue=%s",
        config.host,
        config.port,
        config.mom_host,
        config.transactions_queue,
        config.accounts_queue,
        config.result_queue,
    )


class Gateway:
    def __init__(self, gateway_config: GatewayConfig):
        self.server_host = gateway_config.host
        self.server_port = gateway_config.port
        self.mom_host = gateway_config.mom_host
        self.transactions_queue_name = gateway_config.transactions_queue
        self.accounts_queue_name = gateway_config.accounts_queue
        self.result_queue_name = gateway_config.result_queue

        self.transactions_mw = None
        self.accounts_mw = None
        self.result_mw = None

        self.server_socket = None
        self.running = False

        self._send_lock = threading.Lock()
        self._registry_lock = threading.Lock()
        self._client_registry = {}
        self._client_threads = []
        self._result_thread = None

    def _setup_middleware(self):
        self.transactions_mw = MessageMiddlewareQueueRabbitMQ(
            self.mom_host, self.transactions_queue_name
        )
        self.accounts_mw = MessageMiddlewareQueueRabbitMQ(
            self.mom_host, self.accounts_queue_name
        )
        self.result_mw = MessageMiddlewareQueueRabbitMQ(
            self.mom_host, self.result_queue_name
        )

    def _dispatch_result(self, message, ack, _nack):
            try:
                decoded = deserialize(message)
            except Exception:
                logging.exception("Error decoding result message; discarding")
                ack()
                return

            client_id = decoded.get("client")

            with self._registry_lock:
                entry = self._client_registry.get(client_id)

            if entry is None:
                logging.warning("No client registered for id %s; discarding result", client_id)
                ack()
                return

            try:
                entry["tcp"].send_bytes(message)
            except Exception:
                logging.exception("Error forwarding result to client %s", client_id)
                entry["done"].set()
                ack()
                return

            ack()

            if decoded.get("type") == "eof":
                logging.info("Received EOF on result queue for client %s", client_id)
                entry["done"].set()

    def handle_client_request(self, client_socket, msg_handler):
        tcp = TCPSocket(client_socket)
        client_id = str(uuid.uuid4())
        done = threading.Event()

        with self._registry_lock:
                self._client_registry[client_id] = {
                    "tcp": tcp,
                    "done": done,
                }

        logging.info("Registered client %s", client_id)

        try:
            while True:
                try:
                    raw_bytes = client_socket.recv_bytes()
                except ConnectionError:
                    logging.info("The client has disconnected")
                    break
                
                message = deserialize(raw_bytes)
                msg_type = message.get("type")

                if msg_type == "raw_transactions":
                    txs = message.get("payload", {}).get("batch", [])
                    serialized_message = serialize(build_raw_transactions_message(
                        client=client_id,
                        msg_id=str(uuid.uuid4()),
                        batch=txs,
                    ))

                    with self._send_lock:
                        self.transactions_mw.send(serialized_message)

                elif msg_type == "raw_accounts":
                    accounts = message.get("payload", {}).get("batch", [])
                    serialized_message = serialize(build_raw_accounts_message(
                        client=client_id,
                        msg_id=str(uuid.uuid4()),
                        batch=accounts,
                    ))
                    with self._send_lock:
                        self.accounts_mw.send(serialized_message)

            logging.info("Inbound EOF received for client %s; forwarding EOF downstream", client_id)
            eof_message = serialize(
                build_eof_message(client=client_id, msg_id=str(uuid.uuid4()))
            )

            with self._send_lock:
                self.transactions_mw.send(eof_message)
                self.accounts_mw.send(eof_message)

            done.wait()
            logging.info("Pipeline complete for client %s; closing client socket", client_id)
                
        except socket.error:
            logging.error("The connection with the server was lost")
        except Exception as e:
            logging.error(e)
        finally:
            with self._registry_lock:
                self._client_registry.pop(client_id, None)
            client_socket.close()

    def handle_client_response(self):
            self.result_mw.start_consuming(self._dispatch_result)

    @staticmethod
    def handle_sigterm(server_socket, client_list, sigterm_received, signum, frame):
        """Static method because it doesn't need 'self' state, it only acts on passed arguments."""
        try:
            server_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass # Ignore if socket is already closed

        for [_, client_socket] in client_list:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        sigterm_received.value = 1

    def run(self):
        logging.info("Starting Gateway...")
        self.running = True

        self._setup_middleware()

        self._result_thread = threading.Thread(target=self._handle_client_response, deaemon=True)
        self._result_thread.start()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_host, self.server_port))
        self.server_socket.listen(8)

        logging.info(
            "Listening for connections on %s:%s",
            self.server_host,
            self.server_port,
        )

        try:
            while self.running:
                try:
                    client_socket_socket, addr = self.server_socket.accept()
                except OSError:
                    break

                logging.info("A new client has connected from %s", addr)

                client_thread = threading.Thread(
                    target=self.handle_client_request,
                    args=(client_socket_socket, message_handler.MessageHandler()),
                    daemon=True,
                )
                client_thread.start()
                self._client_threads.append(client_thread)
        except Exception as e:
            logging.error(f"Unexpected error in accept loop: {e}")
        finally:
            self._close_resources()
            
        return 0

def main():
    config = init_config()
    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO)
    )
    log_config(config)

    gateway = Gateway(config)

    def handle_sigterm(signum, frame):
        logging.info("Received shutdown signal %s", signum)
        gateway.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    return gateway.run()

if __name__ == "__main__":
    raise SystemExit(main())