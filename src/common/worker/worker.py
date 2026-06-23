import logging
import signal
from abc import ABC, abstractmethod
import zlib

from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.common.middleware.middleware_rabbitmq import CONSUMER_HEARTBEAT
from src.common.communication.internal import (
    build_batch_message,
    build_eof_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)
from src.common.eof import EofCoordinator
from src.common.state_manager import WorkerStateManager
from src.common.duplicate_handler import DuplicateHandler


class BaseWorker(ABC):
    """
    Handles all RabbitMQ infrastructure, round-robin routing, and EOF coordination.
    Subclasses only need to implement `process_batch` and `flush_state`.
    """

    def __init__(self, config):
        self.config = config
        self._running = False
        self.strategy = config.strategy

        # Identidad estable del emisor (no un uuid random): viaja en cada mensaje
        # saliente para que el receptor pueda deduplicar por (sender, msg_id).
        self.sender_id = f"{config.stage_name}_{config.worker_id}"

        # Contador monotonico por sender: cada mensaje saliente (datos y EOF)
        # lleva un msg_id entero creciente arrancando en 0. Es la base del dedup
        # por (sender, msg_id).
        self.msg_counter = 0

        self.eof_state_manager = WorkerStateManager(
            base_dir="/app/state",
            stage_name=f"{self.config.stage_name}_eof",
            worker_id=self.config.worker_id,
        )

        self.duplicate_handler = DuplicateHandler()

    def start(self) -> None:
        logging.info(f"Starting {self.__class__.__name__}...")
        self._running = True

        self.eof_coordinator = EofCoordinator(
            expected_eofs=self.config.expected_eofs,
            on_flush=self._internal_on_flush,
            state_manager=self.eof_state_manager,
        )

        routing_keys = [f"worker_{self.config.worker_id}", "eof_broadcast"]

        queue_name = f"{self.config.stage_name}_worker_{self.config.worker_id}"

        self.input_exchange = MessageMiddlewareExchangeRabbitMQ(
            host=self.config.mom_host,
            exchange_name=self.config.input_exchange,
            routing_keys=routing_keys,
            queue_name=queue_name,
            heartbeat=CONSUMER_HEARTBEAT,  # consumer: RabbitMQ detecta caidas y re-encola
        )

        # Multi-output: un worker puede fanout-ear el mismo batch a varias ramas
        # (ej. Filtro USD -> Q1, Q2, router_fecha). Si la config solo trae un
        # `output_exchange`, se comporta igual que antes (lista de 1).
        output_names = getattr(self.config, "output_exchanges", None) or [
            self.config.output_exchange
        ]
        self.output_exchanges = [
            MessageMiddlewareExchangeRabbitMQ(self.config.mom_host, name)
            for name in output_names
        ]
        # Compat: codigo que todavia referencia self.output_exchange usa la 1ra.
        self.output_exchange = self.output_exchanges[0]

        self.input_exchange.start_consuming(self._on_message)

    def _on_message(self, message: bytes, ack, nack) -> None:
        try:
            decoded = deserialize(message)
            msg_type = decoded.get("type")
            client_id = decoded.get("client")
            sender = decoded.get("sender")
            msg_id = decoded.get("msg_id")

            if self.duplicate_handler.is_duplicate(client_id, sender, msg_id):
                logging.info(
                    "Duplicate message from sender %s for client %s with msg_id %s. Acknowledging without processing.",
                    sender,
                    client_id,
                    msg_id,
                )
                ack()
                return

            if msg_type == "eof":
                logging.info("Received EOF from upstream for client %s", client_id)
                self.eof_coordinator.handle_eof(client_id)
            else:
                logging.info(
                    "Received message of type %s for client %s", msg_type, client_id
                )

                self.duplicate_handler.mark_seen(client_id, sender, msg_id)
                batch = decoded.get("payload", {}).get("batch", [])
                self.process_batch(client_id, batch, msg_type)

            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def send_downstream(
        self, client_id: str, message: dict, shard_routing_key: str = None
    ) -> None:
        """Serializes and routes a fully built message to the next stage.

        Primitiva de despacho de bajo nivel: estampa sender/msg_id, elige la
        routing key y hace fan-out. Los subclases normalmente usan send() o
        send_groups(); join la usa directamente via su callback.
        """
        if not message:
            return
        logging.info(
            "Routing message downstream for client %s with routing strategy %s",
            client_id,
            self.config.routing_strategy,
        )
        # Copia para no mutar el dict que nos pasaron (lo estampa el padre).
        message = dict(message)
        msg_id = self._next_msg_id()
        message["sender"] = self.sender_id
        message["msg_id"] = msg_id
        out_msg = serialize(message)

        if self.config.routing_strategy == "round_robin":
            # Round-robin determinista: el worker destino sale de msg_id % N.
            # Como msg_id es un contador monotonico, el reparto es exacto y, ante
            # una reentrega, el mismo mensaje cae siempre en el mismo worker.
            target_worker = (msg_id % self.config.num_downstream_workers) + 1
            routing_key = f"worker_{target_worker}"

        elif self.config.routing_strategy == "sharded":
            routing_key = shard_routing_key

        elif self.config.routing_strategy == "broadcast":
            # Fanout: todos los workers downstream bindean "data_broadcast" y
            # reciben una copia del mensaje (mismo mecanismo que "eof_broadcast").
            routing_key = "data_broadcast"

        else:
            raise ValueError(
                f"Unknown routing strategy: {self.config.routing_strategy}"
            )

        # Fanout a cada rama downstream (una sola si no es multi-output).
        for exchange in self.output_exchanges:
            # TODO: hacer roundrobin a cada output exchange - Ver que no se este mandando a todos los workers de cada stage
            exchange.send(out_msg, routing_key=routing_key)

    def _internal_on_flush(self, client_id: str) -> None:
        """Triggered by EofCoordinator. Flushes subclass state, then broadcasts EOF."""
        self.flush_state(client_id)

        logging.info("Broadcasting EOF downstream for client %s", client_id)
        self.duplicate_handler.clear_client(client_id)

        eof_message = build_eof_message(
            client=client_id, msg_id=self._next_msg_id(), sender=self.sender_id
        )
        eof_msg = serialize(eof_message)
        for exchange in self.output_exchanges:
            exchange.send(eof_msg, routing_key="eof_broadcast")

    def stop(self) -> None:
        self._running = False

        self.input_exchange.stop_consuming()
        self.input_exchange.close()
        for exchange in self.output_exchanges:
            exchange.close()

    def _next_msg_id(self) -> int:
        """Devuelve el proximo msg_id monotonico de este sender y avanza el contador."""
        msg_id = self.msg_counter
        self.msg_counter += 1
        return msg_id

    def send(self, client_id: str, batch: list, message_type: str = "batch") -> None:
        """Emite un unico batch aguas abajo. La routing key la decide la
        routing_strategy (round_robin / broadcast). El subclase solo aporta las
        filas y el tipo; el armado del mensaje y el ruteo los hace el padre."""
        if not batch:
            return
        message = self._build_message(message_type, client_id, batch)
        self.send_downstream(client_id, message)

    def send_groups(
        self, client_id: str, groups: list, message_type: str = "batch"
    ) -> None:
        """Emite filas agrupadas. `groups` es una lista de (shard_key, batch).

        Si la routing_strategy es 'sharded', agrupa por worker fisico
        (_sharded_route) y manda un mensaje por worker. Si no, aplana todos los
        grupos en un unico mensaje. Asi el subclase no ramifica segun la
        estrategia ni calcula routing keys."""
        if self.config.routing_strategy == "sharded":
            physical_groups: dict[str, list] = {}
            for shard_key, batch in groups:
                if not batch:
                    continue
                routing_key = self._sharded_route(str(shard_key))
                physical_groups.setdefault(routing_key, []).extend(batch)
            for routing_key, combined_batch in physical_groups.items():
                message = self._build_message(message_type, client_id, combined_batch)
                self.send_downstream(
                    client_id, message, shard_routing_key=routing_key
                )
        else:
            flat_batch: list = []
            for _shard_key, batch in groups:
                flat_batch.extend(batch)
            self.send(client_id, flat_batch, message_type)

    def _build_message(self, message_type: str, client_id: str, batch: list) -> dict:
        """Construye el mensaje del tipo pedido. sender/msg_id los estampa
        send_downstream, asi que aca no se setean."""
        if message_type == "raw_transactions":
            return build_raw_transactions_message(
                client=client_id, msg_id=None, batch=batch
            )
        return build_batch_message(message_type, client=client_id, batch=batch)

    def _sharded_route(self, shard_key: str) -> str:
        """Worker fisico destino para una clave logica (sharding por hash)."""
        hash_val = zlib.crc32(shard_key.encode("utf-8"))
        target_worker = (hash_val % self.config.num_downstream_workers) + 1
        return f"worker_{target_worker}"

    @abstractmethod
    def process_batch(self, client_id: str, batch: list, msg_type: str) -> None:
        """Procesa un batch entrante ya extraido del payload."""
        pass

    @abstractmethod
    def flush_state(self, client_id: str) -> None:
        """Send any final aggregated state before the EOF is forwarded."""
        pass


class StreamWorker(BaseWorker):
    """Base para los workers ad-hoc (router e historical_filter) que necesitan
    una topologia de input/output propia y que emiten en el flush, en vez del
    fanout estandar de BaseWorker.

    Reusa de BaseWorker la identidad del emisor (sender_id/msg_counter/
    _next_msg_id), el eof_state_manager y el armado de mensajes, pero pisa
    start/_on_message/stop para no usar su modelo de salida. En esta rama NO se
    usan send/send_groups/send_downstream/output_exchanges/duplicate_handler.

    Hooks que implementa cada subclase:
      - input_routing_keys(): keys que bindea el input (default: worker_id + eof).
      - setup_outputs()/close_outputs(): crear/cerrar los exchanges de salida.
      - handle_data(client_id, msg_type, batch): procesar un batch de datos.
      - on_flush(client_id): emitir el resultado final + EOF (lo dispara
        EofCoordinator cuando llegaron todos los EOFs esperados).
    """

    def __init__(self, config):
        super().__init__(config)
        self.input_exchange = None

    def start(self) -> None:
        logging.info("Starting %s...", self.__class__.__name__)
        self._running = True

        self.setup_outputs()

        self.eof_coordinator = EofCoordinator(
            expected_eofs=self.config.expected_eofs,
            on_flush=self.on_flush,
            state_manager=self.eof_state_manager,
        )

        self.input_exchange = MessageMiddlewareExchangeRabbitMQ(
            host=self.config.mom_host,
            exchange_name=self.config.input_exchange,
            routing_keys=self.input_routing_keys(),
            heartbeat=CONSUMER_HEARTBEAT,  # consumer: RabbitMQ detecta caidas y re-encola
        )

        logging.info(
            "%s listening on %s", self.__class__.__name__, self.config.input_exchange
        )
        self.input_exchange.start_consuming(self._on_message)

    def _on_message(self, message, ack, nack) -> None:
        try:
            decoded = deserialize(message)
            msg_type = decoded.get("type")
            client_id = decoded.get("client")

            if msg_type == "eof":
                logging.info("EOF received for client %s", client_id)
                self.eof_coordinator.handle_eof(client_id)
            else:
                batch = decoded.get("payload", {}).get("batch", [])
                self.handle_data(client_id, msg_type, batch)
            ack()
        except Exception:
            logging.exception("Error processing message; nack")
            nack()

    def stop(self) -> None:
        self._running = False
        if self.input_exchange:
            self.input_exchange.stop_consuming()
            self.input_exchange.close()
        self.close_outputs()

    def input_routing_keys(self) -> list:
        """Keys que bindea el input exchange. Default: la cola round-robin de
        este worker mas el broadcast de EOF. Las subclases que reciben mas de un
        stream (ej. historical_filter con data_broadcast) lo pisan."""
        return [f"worker_{self.config.worker_id}", "eof_broadcast"]

    # process_batch/flush_state son @abstractmethod en BaseWorker, pero en esta
    # rama el dispatch entra por handle_data/on_flush. Los implementamos para que
    # las hojas sean instanciables; no se invocan.
    def process_batch(self, client_id: str, batch: list, msg_type: str) -> None:
        raise NotImplementedError

    def flush_state(self, client_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def setup_outputs(self) -> None:
        """Crear los exchanges de salida propios de la subclase."""
        pass

    @abstractmethod
    def close_outputs(self) -> None:
        """Cerrar los exchanges de salida creados en setup_outputs."""
        pass

    @abstractmethod
    def handle_data(self, client_id: str, msg_type: str, batch: list) -> None:
        """Procesar un batch de datos (no EOF) recien llegado."""
        pass

    @abstractmethod
    def on_flush(self, client_id: str) -> None:
        """Emitir el resultado final y propagar el EOF aguas abajo."""
        pass


def run_worker(worker) -> int:
    """Registra los handlers de SIGTERM/SIGINT que paran al worker y arranca el
    consumo. Centraliza el boilerplate identico de cada main()."""

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        worker.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    worker.start()
    return 0
