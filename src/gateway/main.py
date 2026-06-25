import logging
import os
import signal
import socket
import threading
from dataclasses import dataclass

from src.common import fail_recovery
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.gateway.client_handler import ClientHandler, ResultConsumer, WorkerRouter
from src.gateway.liveness import ClientReaper
from src.gateway.state import ClientRegistry, GatewayState


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
    """Orquestador del gateway."""

    def __init__(self, gateway_config):
        self.config = gateway_config
        self.server_host = gateway_config.host
        self.server_port = gateway_config.port
        self.mom_host = gateway_config.mom_host
        self.sender_id = "gateway"
        # El msg_id lo asigna el cliente y viaja en cada mensaje; el gateway lo
        # reutiliza tal cual aguas abajo. Por eso no mantiene contador propio:
        # nada que persistir y nada que reiniciar mal tras una caida.
        self.expected_results = gateway_config.expected_results

        self.server_socket = None
        self.running = False
        # Seam de graceful shutdown: lo observan los loops (accept y por-cliente)
        # para salir ordenadamente.
        self._shutdown = threading.Event()

        # Estado durable del gateway (UUIDs asignados, cursor de ingreso por
        # cliente y progreso de EOFs de resultado). Se carga del disco al
        # construirse (recovery) y lo comparten el ingress (lo escribe/consulta) y
        # el egress (persiste los EOF de resultado).
        self.state = GatewayState()
        # El registro de sesiones usa el state como autoridad de identidad durable.
        self.registry = ClientRegistry(store=self.state)
        self.router = None
        self.result_consumer = None
        # Detector/declarador de caidas de cliente (wipe downstream + limpieza de
        # estado). Necesita el router (para el wipe), asi que se construye en
        # _setup_middleware.
        self.reaper = None

        # Middlewares: 3 de salida (a workers) + 1 de entrada (resultados).
        self.transactions_usd_mw = None
        self.transactions_date_mw = None
        self.accounts_mw = None
        self.result_mw = None

        self._client_threads = []
        self._result_thread = None

    def _setup_middleware(self):
        self.transactions_usd_mw = MessageMiddlewareExchangeRabbitMQ(
            self.mom_host, self.config.transactions_usd_exchange, exchange_type="direct"
        )
        self.transactions_date_mw = MessageMiddlewareExchangeRabbitMQ(
            self.mom_host,
            self.config.transactions_date_exchange,
            exchange_type="direct",
        )
        self.accounts_mw = MessageMiddlewareExchangeRabbitMQ(
            self.mom_host, self.config.accounts_exchange, exchange_type="direct"
        )
        self.result_mw = MessageMiddlewareExchangeRabbitMQ(
            host=self.mom_host,
            exchange_name=self.config.result_exchange,
            routing_keys=["worker_1"],
            queue_name="gateway_result_queue",
        )

        self.router = WorkerRouter(
            transactions_usd_mw=self.transactions_usd_mw,
            transactions_date_mw=self.transactions_date_mw,
            accounts_mw=self.accounts_mw,
            transactions_usd_workers=self.config.transactions_usd_workers,
            transactions_date_workers=self.config.transactions_date_workers,
            accounts_workers=self.config.accounts_workers,
        )
        self.reaper = ClientReaper(self.registry, self.router, self.state)
        self.result_consumer = ResultConsumer(
            self.result_mw,
            self.registry,
            self.expected_results,
            self.state,
            self.reaper,
        )

    def run(self):
        logging.info("Starting Gateway...")
        self.running = True

        self._start_fail_detection()

        self._setup_middleware()

        # Tras un restart, los clientes persistidos aparecen desconectados: les
        # damos una ventana de gracia para reconectar antes de darlos por caidos.
        self.reaper.arm_recovery_timers()

        self._result_thread = threading.Thread(
            target=self.result_consumer.run, daemon=True
        )
        self._result_thread.start()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.server_host, self.server_port))
        self.server_socket.listen(10)

        logging.info(
            "Listening for connections on %s:%s",
            self.server_host,
            self.server_port,
        )

        try:
            while self.running:
                try:
                    client_socket, addr = self.server_socket.accept()
                except OSError:
                    break

                logging.info("A new client has connected from %s", addr)

                handler = ClientHandler(
                    client_socket=client_socket,
                    registry=self.registry,
                    router=self.router,
                    sender_id=self.sender_id,
                    shutdown_event=self._shutdown,
                    state=self.state,
                    expected_results=self.expected_results,
                    reaper=self.reaper,
                )
                client_thread = threading.Thread(target=handler.run, daemon=True)
                client_thread.start()
                self._client_threads.append(client_thread)
        except Exception as e:
            logging.error(f"Unexpected error in accept loop: {e}")
        finally:
            self._close_resources()

        return 0

    def stop(self):
        """Graceful shutdown: dejamos de aceptar clientes y desbloqueamos el
        accept() cerrando el socket de escucha. El loop de run() sale solo y su
        finally se encarga de liberar el resto de los recursos."""
        logging.info("Stopping Gateway...")
        self.running = False
        self._shutdown.set()
        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # El socket ya estaba cerrado o sin conexion

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
        # Seam de graceful shutdown: aca ira el flush de sesiones persistidas
        # antes de cerrar. Por ahora solo joineamos los threads por-cliente.
        for t in self._client_threads:
            t.join(timeout=5)

    def _start_fail_detection(self):
        """Start fail detection.
        node_id is the worker_id (unique within the stage) and the peers come from the environment variables.
        Node.start() blocks (runs its monitoring loop), so it runs in a daemon thread to
        avoid blocking message consumption.
        """
        self.fd_node = fail_recovery.node_from_env()
        threading.Thread(
            target=self.fd_node.start, daemon=True, name="fail-detection"
        ).start()
        logging.info("Fail detection daemon started")


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
