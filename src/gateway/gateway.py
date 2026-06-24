import logging
import socket
import threading

from src.common import fail_recovery
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ

from src.gateway.egress import ResultConsumer
from src.gateway.identity import UuidRegistry
from src.gateway.ingress import ClientHandler
from src.gateway.ingress_cursor import IngressCursorStore
from src.gateway.result_progress import GatewayResultProgress
from src.gateway.router import WorkerRouter
from src.gateway.sessions import ClientRegistry


class Gateway:
    """Orquestador del gateway. Cablea las piezas (registro de sesiones, router
    de salida, consumidor de resultados), levanta los threads y maneja el ciclo
    de vida. La logica de cada flujo vive en sus modulos:
    entrada -> ingress.ClientHandler, salida -> egress.ResultConsumer.
    """

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

        # Registro de sesiones con autoridad de identidad durable: el store
        # persiste a disco los UUIDs asignados (sobrevive a una caida del
        # gateway). Se carga del disco al construirse (recovery).
        self.registry = ClientRegistry(store=UuidRegistry())
        # Progreso durable de EOFs de resultado por cliente (sobrevive a una
        # caida del gateway). Compartido entre el egress (lo persiste) y el
        # ingress (lo consulta para cerrar clientes ya completos al reanudar).
        self.result_progress = GatewayResultProgress()
        # Cursor durable de ingreso por cliente (uuid -> ultimo msg_id reenviado
        # downstream): base de la reanudacion del streaming de datos.
        self.ingress_cursor = IngressCursorStore()
        self.router = None
        self.result_consumer = None

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
        self.result_consumer = ResultConsumer(
            self.result_mw,
            self.registry,
            self.expected_results,
            self.result_progress,
        )

    def run(self):
        logging.info("Starting Gateway...")
        self.running = True

        self._start_fail_detection()

        self._setup_middleware()

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
                    progress=self.result_progress,
                    expected_results=self.expected_results,
                    cursor=self.ingress_cursor,
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
