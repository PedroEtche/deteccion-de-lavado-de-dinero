import logging
from abc import ABC, abstractmethod
import uuid
import zlib

from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.common.communication.internal import (
    build_eof_message,
    deserialize,
    serialize,
)
from src.common.eof import EofCoordinator

class BaseWorker(ABC):
    """
    Handles all RabbitMQ infrastructure, round-robin routing, and EOF coordination.
    Subclasses only need to implement `process_data` and `flush_state`.
    """
    def __init__(self, config):
        self.config = config
        self.current_downstream_worker = 1
        self._running = False
        self.strategy = config.strategy    

    def start(self) -> None:
        logging.info(f"Starting {self.__class__.__name__}...")
        self._running = True
        
        self.eof_coordinator = EofCoordinator(
            expected_eofs=self.config.expected_eofs,
            on_flush=self._internal_on_flush,
        )
        
        routing_keys = [
            f"worker_{self.config.worker_id}",
            "eof_broadcast"
        ]
        self.input_exchange = MessageMiddlewareExchangeRabbitMQ(
            host=self.config.mom_host, 
            exchange_name=self.config.input_exchange, 
            routing_keys=routing_keys
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

            if msg_type == "eof":
                logging.info("Received EOF from upstream for client %s", client_id)
                self.eof_coordinator.handle_eof(client_id)
            else:
                logging.info("Received message of type %s for client %s", msg_type, client_id)
                self.process_data(client_id, decoded["msg_id"], msg_type, decoded["payload"])
            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def send_downstream(self, client_id: str, message: dict, shard_routing_key: str = None) -> None:
        """Serializes and routes a fully built message to the next stage."""
        if not message:
            return
        logging.info("Routing message downstream for client %s with routing strategy %s", client_id, self.config.routing_strategy)
        out_msg = serialize(message)

        if self.config.routing_strategy == "round_robin":
            routing_key = f"worker_{self.current_downstream_worker}"
            self.current_downstream_worker = (self.current_downstream_worker % self.config.num_downstream_workers) + 1

        elif self.config.routing_strategy == "sharded":
            routing_key = shard_routing_key

        elif self.config.routing_strategy == "broadcast":
            # Fanout: todos los workers downstream bindean "data_broadcast" y
            # reciben una copia del mensaje (mismo mecanismo que "eof_broadcast").
            routing_key = "data_broadcast"

        else:
            raise ValueError(f"Unknown routing strategy: {self.config.routing_strategy}")

        # Fanout a cada rama downstream (una sola si no es multi-output).
        for exchange in self.output_exchanges:
            exchange.send(out_msg, routing_key=routing_key)

    def _internal_on_flush(self, client_id: str) -> None:
        """Triggered by EofCoordinator. Flushes subclass state, then broadcasts EOF."""
        self.flush_state(client_id)

        logging.info("Broadcasting EOF downstream for client %s", client_id)
        
        eof_msg = serialize(build_eof_message(client=client_id, msg_id=str(uuid.uuid4())))
        for exchange in self.output_exchanges:
            exchange.send(eof_msg, routing_key="eof_broadcast")

    def stop(self) -> None:
        self._running = False

        self.input_exchange.stop_consuming()
        self.input_exchange.close()
        for exchange in self.output_exchanges:
            exchange.close()
    
    def get_sharded_route(self, shard_key: str) -> str:
        """Helper for subclasses that need to pre-batch data by physical route."""
        hash_val = zlib.crc32(shard_key.encode("utf-8"))
        target_worker = (hash_val % self.config.num_downstream_workers) + 1
        return f"worker_{target_worker}"

    @abstractmethod
    def process_data(self, client_id: str, msg_id: str, msg_type: str, payload: dict) -> None:
        """Process incoming data."""
        pass

    @abstractmethod
    def flush_state(self, client_id: str) -> None:
        """Send any final aggregated state before the EOF is forwarded."""
        pass