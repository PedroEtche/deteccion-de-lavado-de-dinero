import logging
import signal
import threading
import zlib
from abc import ABC, abstractmethod

from src.common import fail_recovery
from src.common.communication.internal import (
    build_batch_message,
    build_eof_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)
from src.common.duplicate_handler import DuplicateHandler
from src.common.eof import EofCoordinator
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.common.middleware.middleware_rabbitmq import CONSUMER_HEARTBEAT
from src.common.state_manager import WorkerStateManager


class BaseWorker(ABC):
    """
    Core infrastructure worker. Handles all AMQP connections, fail recovery, 
    EOF coordination, deduplication, and downstream routing logic.
    """

    def __init__(self, config):
        self.config = config
        self._running = False
        self.strategy = config.strategy

        # Identidad estable del emisor 
        self.sender_id = f"{config.stage_name}_{config.worker_id}"

        # Contador monotonico usado por StatefulWorkers (para datos) 
        # y por todos los workers (para emitir EOFs).
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

        self._start_fail_detection()

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
            heartbeat=CONSUMER_HEARTBEAT,
        )

        output_names = getattr(self.config, "output_exchanges", None) or [
            self.config.output_exchange
        ]
        self.output_exchanges = [
            MessageMiddlewareExchangeRabbitMQ(self.config.mom_host, name)
            for name in output_names
        ]
        self.output_exchange = self.output_exchanges[0]

        self.input_exchange.start_consuming(self._on_message)

    def _start_fail_detection(self):
        self.fd_node = fail_recovery.node_from_env()
        threading.Thread(
            target=self.fd_node.start, daemon=True, name="fail-detection"
        ).start()
        logging.info("Fail detection daemon started")

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
                    sender, client_id, msg_id,
                )
                ack()
                return

            if msg_type == "eof":
                logging.info("Received EOF from upstream for client %s", client_id)
                self.eof_coordinator.handle_eof(client_id)
            else:
                logging.info("Received message of type %s for client %s", msg_type, client_id)
                batch = decoded.get("payload", {}).get("batch", [])
                
                self._dispatch_payload(client_id, batch, msg_type, msg_id, sender)

            self.duplicate_handler.mark_seen(client_id, sender, msg_id)
            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    @abstractmethod
    def _dispatch_payload(self, client_id: str, batch: list, msg_type: str, msg_id: int, sender: str) -> None:
        """Subclasses define how they handle incoming signatures."""
        pass

    def _internal_on_flush(self, client_id: str) -> None:
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
        msg_id = self.msg_counter
        self.msg_counter += 1
        return msg_id

    def _route_and_send(self, client_id: str, message: dict, msg_id: int, shard_routing_key: str = None) -> None:
        """Centralized routing logic used by both Stateful and Stateless subclasses."""
        out_msg = serialize(message)

        if self.config.routing_strategy == "round_robin":
            target_worker = (msg_id % self.config.num_downstream_workers) + 1
            routing_key = f"worker_{target_worker}"
        elif self.config.routing_strategy == "sharded":
            routing_key = shard_routing_key
        elif self.config.routing_strategy == "broadcast":
            routing_key = "data_broadcast"
        else:
            raise ValueError(f"Unknown routing strategy: {self.config.routing_strategy}")

        for exchange in self.output_exchanges:
            exchange.send(out_msg, routing_key=routing_key)

    def _sharded_route(self, shard_key: str) -> str:
        hash_val = zlib.crc32(shard_key.encode("utf-8"))
        target_worker = (hash_val % self.config.num_downstream_workers) + 1
        return f"worker_{target_worker}"

    def _build_message(self, message_type: str, client_id: str, batch: list, msg_id: int, sender: str) -> dict:
        if message_type == "raw_transactions":
            return build_raw_transactions_message(client=client_id, msg_id=msg_id, batch=batch, sender=sender)
        return build_batch_message(message_type, client=client_id, batch=batch, msg_id=msg_id, sender=sender)

    @abstractmethod
    def flush_state(self, client_id: str) -> None:
        pass


class StatefulWorker(BaseWorker):
    """
    Worker that buffers or aggregates data. Because it generates brand new 
    data payloads, it generates its own `msg_id` and overwrites the `sender`.
    """

    def _dispatch_payload(self, client_id: str, batch: list, msg_type: str, msg_id: int, sender: str) -> None:
        # Ignore upstream identity, stateful workers only care about the data
        self.process_batch(client_id, batch, msg_type)

    def send(self, client_id: str, batch: list, message_type: str = "batch") -> None:
        if not batch: return
        msg_id = self._next_msg_id()
        message = self._build_message(message_type, client_id, batch, msg_id=msg_id, sender=self.sender_id)
        self._route_and_send(client_id, message, msg_id)

    def send_groups(self, client_id: str, groups: list, message_type: str = "batch") -> None:
        if self.config.routing_strategy == "sharded":
            physical_groups: dict[str, list] = {}
            for shard_key, batch in groups:
                if not batch: continue
                routing_key = self._sharded_route(str(shard_key))
                physical_groups.setdefault(routing_key, []).extend(batch)
                
            for routing_key, combined_batch in physical_groups.items():
                msg_id = self._next_msg_id()
                message = self._build_message(message_type, client_id, combined_batch, msg_id=msg_id, sender=self.sender_id)
                self._route_and_send(client_id, message, msg_id, shard_routing_key=routing_key)
        else:
            flat_batch: list = []
            for _shard_key, batch in groups:
                flat_batch.extend(batch)
            self.send(client_id, flat_batch, message_type)

    @abstractmethod
    def process_batch(self, client_id: str, batch: list, msg_type: str) -> None:
        pass


class StatelessWorker(BaseWorker):
    """
    Worker that acts as a pure pipe (e.g. Filters). It preserves the exact 
    `msg_id` and `sender` from the upstream worker to ensure deduplication 
    works properly across network retries.
    """

    def _dispatch_payload(self, client_id: str, batch: list, msg_type: str, msg_id: int, sender: str) -> None:
        # Explicitly pass the identity to the subclass so it can be routed downstream
        sender_chain = f"{sender}_{self.sender_id}"

        self.process_batch(client_id, batch, msg_type, msg_id, sender_chain)

    def send(self, client_id: str, batch: list, message_type: str = "batch", msg_id: int = None, sender: str = None) -> None:
        if not batch: return
        
        # Fallback just in case an upstream message was malformed
        if msg_id is None or sender is None:
            msg_id = self._next_msg_id()
            sender = self.sender_id
            
        message = self._build_message(message_type, client_id, batch, msg_id=msg_id, sender=sender)
        self._route_and_send(client_id, message, msg_id)

    def send_groups(self, client_id: str, groups: list, message_type: str = "batch", msg_id: int = None, sender: str = None) -> None:
        if msg_id is None or sender is None:
            msg_id = self._next_msg_id()
            sender = self.sender_id

        if self.config.routing_strategy == "sharded":
            physical_groups: dict[str, list] = {}
            for shard_key, batch in groups:
                if not batch: continue
                routing_key = self._sharded_route(str(shard_key))
                physical_groups.setdefault(routing_key, []).extend(batch)
                
            for routing_key, combined_batch in physical_groups.items():
                message = self._build_message(message_type, client_id, combined_batch, msg_id=msg_id, sender=sender)
                self._route_and_send(client_id, message, msg_id, shard_routing_key=routing_key)
        else:
            flat_batch: list = []
            for _shard_key, batch in groups:
                flat_batch.extend(batch)
            self.send(client_id, flat_batch, message_type, msg_id=msg_id, sender=sender)

    @abstractmethod
    def process_batch(self, client_id: str, batch: list, msg_type: str, msg_id: int, sender: str) -> None:
        pass


class StreamWorker(BaseWorker):
    """Base para los workers ad-hoc (router e historical_filter) que necesitan
    una topologia de input/output propia y que emiten en el flush, en vez del
    fanout estandar de BaseWorker.

    Reusa de BaseWorker la identidad del emisor (sender_id/msg_counter/
    _next_msg_id), el eof_state_manager y el armado de mensajes, pero pisa
    start/stop para no usar su modelo de salida. En esta rama NO se
    usan send/send_groups/send_downstream/output_exchanges.

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

        self._start_fail_detection()

        self.setup_outputs()

        # Al pasar self.on_flush, bypassamos el _internal_on_flush de BaseWorker
        self.eof_coordinator = EofCoordinator(
            expected_eofs=self.config.expected_eofs,
            on_flush=self.on_flush,
            state_manager=self.eof_state_manager,
        )

        queue_name = f"{self.config.stage_name}_worker_{self.config.worker_id}"

        self.input_exchange = MessageMiddlewareExchangeRabbitMQ(
            host=self.config.mom_host,
            exchange_name=self.config.input_exchange,
            routing_keys=self.input_routing_keys(),
            queue_name=queue_name,
            heartbeat=CONSUMER_HEARTBEAT,
        )

        logging.info(
            "%s listening on %s", self.__class__.__name__, self.config.input_exchange
        )
        
        # Usamos el _on_message nativo de BaseWorker para no perder la deduplicación
        self.input_exchange.start_consuming(self._on_message)

    def stop(self) -> None:
        self._running = False
        if self.input_exchange:
            self.input_exchange.stop_consuming()
            self.input_exchange.close()
        self.close_outputs()

    def input_routing_keys(self) -> list:
        """Keys que bindea el input exchange. Default: la cola round-robin de
        este worker mas el broadcast de EOF. Las subclases que reciben mas de un
        stream lo pisan."""
        return [f"worker_{self.config.worker_id}", "eof_broadcast"]
    
    def _dispatch_payload(self, client_id: str, batch: list, msg_type: str, msg_id: int, sender: str) -> None:
        """
        Atrapa el payload validado y deduplicado por BaseWorker y lo redirige
        al método handle_data de los workers ad-hoc.
        """
        self.handle_data(client_id, msg_type, batch, msg_id, sender)

    def flush_state(self, client_id: str) -> None:
        """
        Satisface el método abstracto de BaseWorker. 
        Nunca se ejecuta porque el EofCoordinator apunta directamente a self.on_flush.
        """
        pass

    @abstractmethod
    def setup_outputs(self) -> None:
        """Crear los exchanges de salida propios de la subclase."""
        pass

    @abstractmethod
    def close_outputs(self) -> None:
        """Cerrar los exchanges de salida creados en setup_outputs."""
        pass

    @abstractmethod
    def handle_data(self, client_id: str, msg_type: str, batch: list, msg_id: int, sender: str) -> None:
        """Procesar un batch de datos (no EOF) recien llegado."""
        pass

    @abstractmethod
    def on_flush(self, client_id: str) -> None:
        """Emitir el resultado final y propagar el EOF aguas abajo."""
        pass