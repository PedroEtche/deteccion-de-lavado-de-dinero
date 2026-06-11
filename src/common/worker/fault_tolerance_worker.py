import logging
import uuid
import zlib
from abc import ABC, abstractmethod

from src.common.communication.internal import (
    build_eof_message,
    deserialize,
    serialize,
)
from src.common.eof import EofCoordinator
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ


class FaultToleranceWorker(ABC):
    """
    Base for workers. Handles all RabbitMQ infrastructure,
    round-robin routing, and EOF coordination. Subclasses only need to implement
    `process_data` and `flush_state`.

    Supports a master-slave replication lane: the master republishes every
    message to a fanout exchange before processing it, and the slave consumes only
    from that fanout (so it sees an exact copy of the master's input). The slave
    never emits anything downstream.
    """

    def __init__(self, config):
        self.config = config
        self.current_downstream_worker = 1
        self._running = False
        self.strategy = config.strategy
        self.master = config.role == "master"
        self.replication_exchange = config.replication_exchange
        self.replication = None

    def start(self) -> None:
        logging.info(
            "Starting %s as %s...",
            self.__class__.__name__,
            "master" if self.master else "slave",
        )
        self._running = True

        self.eof_coordinator = EofCoordinator(
            expected_eofs=self.config.expected_eofs,
            on_flush=self._internal_on_flush,
        )

        self.output_exchange = MessageMiddlewareExchangeRabbitMQ(
            self.config.mom_host, self.config.output_exchange
        )

        if self.master:
            routing_keys = [f"worker_{self.config.worker_id}", "eof_broadcast"]
            self.input_exchange = MessageMiddlewareExchangeRabbitMQ(
                host=self.config.mom_host,
                exchange_name=self.config.input_exchange,
                routing_keys=routing_keys,
            )

            # Publisher to the intra-stage replication fanout (if configured).
            if self.replication_exchange:
                self.replication = MessageMiddlewareExchangeRabbitMQ(
                    host=self.config.mom_host,
                    exchange_name=self.replication_exchange,
                    exchange_type="fanout",
                )

            self.input_exchange.start_consuming(self._on_message)
        else:
            # NOTE: Slave: do NOT bind the main input queue. Consume only the replicated
            # copy from the fanout.
            self.replication = MessageMiddlewareExchangeRabbitMQ(
                host=self.config.mom_host,
                exchange_name=self.replication_exchange,
                exchange_type="fanout",
                routing_keys=[""],
            )
            self.replication.start_consuming(self._on_message)

    def _on_message(self, message: bytes, ack, nack) -> None:
        try:
            # Master replicates the raw bytes to the slave(s) BEFORE processing
            # and acking. Order matters: replicate -> process -> ack.
            if self.master and self.replication is not None:
                self.replication.send(message, routing_key="")

            decoded = deserialize(message)
            msg_type = decoded.get("type")
            client_id = decoded.get("client")

            if msg_type == "eof":
                logging.info("Received EOF from upstream for client %s", client_id)
                self.eof_coordinator.handle_eof(client_id)
            else:
                logging.info(
                    "Received message of type %s for client %s", msg_type, client_id
                )
                self.process_data(
                    client_id, decoded["msg_id"], msg_type, decoded["payload"]
                )
                # Master Health check
            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def send_downstream(
        self, client_id: str, message: dict, shard_routing_key: str = ""
    ) -> None:
        """Serializes and routes a fully built message to the next stage."""
        if not self.master:
            # A slave processes the same data as its master; emitting downstream
            # would duplicate the output. Stay silent while acting as slave.
            return
        if not message:
            return
        logging.info(
            "Routing message downstream for client %s with routing strategy %s",
            client_id,
            self.config.routing_strategy,
        )
        out_msg = serialize(message)

        if self.config.routing_strategy == "round_robin":
            routing_key = f"worker_{self.current_downstream_worker}"
            self.current_downstream_worker = (
                self.current_downstream_worker % self.config.num_downstream_workers
            ) + 1

        elif self.config.routing_strategy == "sharded":
            routing_key = shard_routing_key

        else:
            raise ValueError(
                f"Unknown routing strategy: {self.config.routing_strategy}"
            )

        self.output_exchange.send(out_msg, routing_key=routing_key)

    def _internal_on_flush(self, client_id: str) -> None:
        """Triggered by EofCoordinator. Flushes subclass state, then broadcasts EOF."""
        self.flush_state(client_id)

        if not self.master:
            # Slaves send nothing downstream, not even the EOF broadcast.
            return

        logging.info("Broadcasting EOF downstream for client %s", client_id)

        eof_msg = serialize(
            build_eof_message(client=client_id, msg_id=str(uuid.uuid4()))
        )
        self.output_exchange.send(eof_msg, routing_key="eof_broadcast")

    def stop(self) -> None:
        self._running = False

        if self.master:
            self.input_exchange.stop_consuming()
            self.input_exchange.close()

        if self.replication is not None:
            self.replication.stop_consuming()
            self.replication.close()

        self.output_exchange.close()

    def get_sharded_route(self, shard_key: str) -> str:
        """Helper for subclasses that need to pre-batch data by physical route."""
        hash_val = zlib.crc32(shard_key.encode("utf-8"))
        target_worker = (hash_val % self.config.num_downstream_workers) + 1
        return f"worker_{target_worker}"

    @abstractmethod
    def process_data(
        self, client_id: str, msg_id: str, msg_type: str, payload: dict
    ) -> None:
        """Process incoming data."""
        pass

    @abstractmethod
    def flush_state(self, client_id: str) -> None:
        """Send any final aggregated state before the EOF is forwarded."""
        pass
