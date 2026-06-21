import logging
import os
import signal
import socket
import threading
import uuid
from dataclasses import dataclass

from src.common.communication.tcp import TCPSocket
from src.common.middleware import (
    MessageMiddlewareExchangeRabbitMQ,
)
from src.common.communication.internal import (
    build_eof_message,
    build_raw_accounts_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)


@dataclass
class GatewayConfig:
    host: str
    port: int
    mom_host: str
    transactions_usd_exchange: str
    transactions_date_exchange: str
    accounts_exchange: str
    result_exchange: str
    log_level: str
    transactions_usd_workers: int
    transactions_date_workers: int
    accounts_workers: int
    # Cuantos EOF de resultado esperar antes de cerrar el cliente (1 por query
    # que corre). Con varias queries a la vez hay que esperarlas a todas.
    expected_results: int


def init_config():
    return GatewayConfig(
        host=os.getenv("SERVER_HOST", "0.0.0.0"),
        port=int(os.getenv("SERVER_PORT", 5678)),
        mom_host=os.getenv("MOM_HOST", ""),
        transactions_usd_exchange=os.getenv(
            "TRANSACTIONS_USD_EXCHANGE", "transactions_usd_exchange"
        ),
        transactions_date_exchange=os.getenv(
            "TRANSACTIONS_DATE_EXCHANGE", "transactions_date_exchange"
        ),
        accounts_exchange=os.getenv("ACCOUNTS_EXCHANGE", "accounts_exchange"),
        result_exchange=os.getenv("RESULT_EXCHANGE", "result_exchange"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        transactions_usd_workers=int(os.getenv("TRANSACTIONS_USD_WORKERS", 1)),
        transactions_date_workers=int(os.getenv("TRANSACTIONS_DATE_WORKERS", 1)),
        accounts_workers=int(os.getenv("ACCOUNTS_WORKERS", 1)),
        expected_results=int(os.getenv("EXPECTED_RESULTS", 1)),
    )


def log_config(config: GatewayConfig):
    logging.info(
        "Gateway startup with: host=%s | port=%s | mom_host=%s | transactions_exchange=%s | accounts_exchange=%s | result_exchange=%s",
        config.host,
        config.port,
        config.mom_host,
        config.transactions_usd_exchange,
        config.accounts_exchange,
        config.result_exchange,
    )


class Gateway:
    def __init__(self, gateway_config: GatewayConfig):
        self.server_host = gateway_config.host
        self.server_port = gateway_config.port
        self.mom_host = gateway_config.mom_host

        self.transactions_usd_exchange_name = gateway_config.transactions_usd_exchange
        self.transactions_date_exchange_name = gateway_config.transactions_date_exchange
        self.accounts_exchange_name = gateway_config.accounts_exchange
        self.result_exchange_name = gateway_config.result_exchange

        self.transactions_usd_workers = gateway_config.transactions_usd_workers
        self.transactions_usd_current_worker = 1
        self.transactions_usd_mw = None

        self.transactions_date_workers = gateway_config.transactions_date_workers
        self.transactions_date_current_worker = 1
        self.transactions_date_mw = None

        self.accounts_workers = gateway_config.accounts_workers
        self.accounts_current_worker = 1
        self.accounts_mw = None
        self.result_mw = None

        self.expected_results = gateway_config.expected_results

        self.server_socket = None
        self.running = False

        self._send_lock = threading.Lock()
        self._registry_lock = threading.Lock()
        self._client_registry = {}
        self._client_threads = []
        self._result_thread = None

    def _setup_middleware(self):
        self.transactions_usd_mw = MessageMiddlewareExchangeRabbitMQ(
            self.mom_host, self.transactions_usd_exchange_name, exchange_type="direct"
        )
        self.transactions_date_mw = MessageMiddlewareExchangeRabbitMQ(
            self.mom_host, self.transactions_date_exchange_name, exchange_type="direct"
        )
        self.accounts_mw = MessageMiddlewareExchangeRabbitMQ(
            self.mom_host, self.accounts_exchange_name, exchange_type="direct"
        )
        self.result_mw = MessageMiddlewareExchangeRabbitMQ(
            host=self.mom_host,
            exchange_name=self.result_exchange_name,
            routing_keys=["worker_1"],
            queue_name="gateway_result_queue",
        )

    def _dispatch_result(self, message, ack, _nack):
        try:
            decoded = deserialize(message)
        except Exception:
            logging.exception("Error decoding result message; discarding")
            ack()
            return

        client_id = decoded.get("client")
        msg_id = decoded.get("msg_id")

        with self._registry_lock:
            entry = self._client_registry.get(client_id)

        if entry is None:
            logging.warning(
                "No client registered for id %s; discarding result", client_id
            )
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

        if decoded.get("eof"):
            # Cada query que corre manda 1 EOF de resultado al terminar. Con
            # varias queries a la vez hay que esperarlas a todas antes de
            # cerrar el cliente; si no, cortariamos al terminar la primera.
            entry["eofs"] = entry.get("eofs", 0) + 1
            logging.info(
                "Received EOF %d/%d on result queue for client %s",
                entry["eofs"],
                self.expected_results,
                client_id,
            )
            if entry["eofs"] >= self.expected_results:
                entry["done"].set()

    def send_accounts_data(self, serialized_message: bytes):
        """Broadcasts data to accounts workers."""
        with self._send_lock:
            logging.info(
                "Broadcasting accounts message to %d workers", self.accounts_workers
            )
            for worker_id in range(1, self.accounts_workers + 1):
                self.accounts_mw.send(
                    serialized_message,
                    routing_key=f"worker_{worker_id}",
                )

    def send_transactions_data(self, serialized_message: bytes):
        """Sends data via Round-Robin to a specific worker."""
        with self._send_lock:
            logging.info(
                "Routing transactions message to workers with Round-Robin strategy"
            )
            self.transactions_date_mw.send(
                serialized_message,
                routing_key=f"worker_{self.transactions_date_current_worker}",
            )
            self.transactions_usd_mw.send(
                serialized_message,
                routing_key=f"worker_{self.transactions_usd_current_worker}",
            )

        self.transactions_date_current_worker = (
            self.transactions_date_current_worker % self.transactions_date_workers
        ) + 1
        self.transactions_usd_current_worker = (
            self.transactions_usd_current_worker % self.transactions_usd_workers
        ) + 1

    def send_eof(self, eof_message: bytes):
        """Broadcasts EOF to all workers listening to the exchange."""
        routing_key = "eof_broadcast"

        with self._send_lock:
            self.transactions_usd_mw.send(eof_message, routing_key=routing_key)
            self.transactions_date_mw.send(eof_message, routing_key=routing_key)
            self.accounts_mw.send(eof_message, routing_key=routing_key)

    def handle_client_request(self, client_socket):
        tcp = TCPSocket(client_socket)
        client_id = str(uuid.uuid4())
        done = threading.Event()

        with self._registry_lock:
            self._client_registry[client_id] = {
                "tcp": tcp,
                "done": done,
                "eofs": 0,
            }

        logging.info("Registered client %s", client_id)

        try:
            while True:
                try:
                    raw_bytes = tcp.recv_bytes()
                except ConnectionError:
                    logging.info("The client has disconnected")
                    break

                message = deserialize(raw_bytes)
                msg_type = message.get("type")
                logging.info(
                    "Received message of type %s from client %s", msg_type, client_id
                )

                if msg_type == "raw_transactions":
                    txs = message.get("payload", {}).get("batch", [])
                    serialized_message = serialize(
                        build_raw_transactions_message(
                            client=client_id,
                            msg_id=str(uuid.uuid4()),
                            batch=txs,
                        )
                    )

                    self.send_transactions_data(serialized_message)

                elif msg_type == "raw_accounts":
                    accounts = message.get("payload", {}).get("batch", [])
                    logging.info(
                        "sending accounts batch of %d rows for client %s",
                        len(accounts),
                        client_id,
                    )
                    serialized_message = serialize(
                        build_raw_accounts_message(
                            client=client_id,
                            msg_id=str(uuid.uuid4()),
                            batch=accounts,
                        )
                    )
                    self.send_accounts_data(serialized_message)

            logging.info(
                "Inbound EOF received for client %s; forwarding EOF downstream",
                client_id,
            )
            eof_message = serialize(
                build_eof_message(client=client_id, msg_id=str(uuid.uuid4()))
            )

            self.send_eof(eof_message)

            done.wait()
            logging.info(
                "Pipeline complete for client %s; closing client socket", client_id
            )

        except socket.error:
            logging.error("The connection with the server was lost")
        except Exception as e:
            logging.error(e)
        finally:
            with self._registry_lock:
                self._client_registry.pop(client_id, None)
            tcp.close()

    def _close_resources(self):
        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass
        for mw in (
            self.transactions_date_mw,
            self.transactions_usd_mw,
            self.accounts_mw,
            self.result_mw,
        ):
            if mw:
                try:
                    mw.close()
                except Exception:
                    pass
        for t in self._client_threads:
            t.join(timeout=5)

    def handle_client_response(self):
        self.result_mw.start_consuming(self._dispatch_result)

    def stop(self):
        """Graceful shutdown: dejamos de aceptar clientes y desbloqueamos el
        accept() cerrando el socket de escucha. El loop de run() sale solo y
        su finally se encarga de liberar el resto de los recursos."""
        logging.info("Stopping Gateway...")
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # El socket ya estaba cerrado o sin conexion

    def run(self):
        logging.info("Starting Gateway...")
        self.running = True

        self._setup_middleware()

        self._result_thread = threading.Thread(
            target=self.handle_client_response, daemon=True
        )
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
                    args=(client_socket_socket,),
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
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    logging.getLogger("pika").setLevel(logging.WARNING)
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
