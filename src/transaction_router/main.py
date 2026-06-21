import logging
import os
import signal
import uuid
from dataclasses import dataclass
from typing import List

from src.common.communication.internal import (
    build_eof_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)
from src.common.eof import EofCoordinator
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.common.middleware.middleware_rabbitmq import CONSUMER_HEARTBEAT
from src.common.utils import load_yaml_config
from src.common.state_manager import WorkerStateManager

from .routes import Route, build_routes

CONFIG_PATH = "./config.yaml"


@dataclass
class RouterConfig:
    mom_host: str
    input_exchange: str
    routes: List[Route]
    expected_eofs: int
    worker_id: int
    log_level: str
    stage_name: str


def init_config() -> RouterConfig:
    data = load_yaml_config(CONFIG_PATH)
    return RouterConfig(
        mom_host=data.get("mom_host", "rabbitmq"),
        input_exchange=data.get("input", "transactions_exchange"),
        routes=build_routes(data.get("routes", [])),
        expected_eofs=int(os.getenv("EOF_EXPECTED", "1")),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        stage_name=os.environ.get("STAGE_NAME", "transaction_router"),
    )


def log_config(config: RouterConfig) -> None:
    logging.info(
        "TransactionRouter startup: mom_host=%s | input=%s | routes=%s | expected_eofs=%d | worker_id=%d",
        config.mom_host,
        config.input_exchange,
        [r.name for r in config.routes],
        config.expected_eofs,
        config.worker_id,
    )


class TransactionRouter:
    """
    Consumes raw transactions from the gateway, evaluates each tx against
    every configured route, and emits one sub-batch per matching route to
    that route's output exchange.
    """

    def __init__(self, config: RouterConfig):
        self.config = config
        self.input_mw = None

        self.eof_state_manager = WorkerStateManager(
            base_dir="/app/state",
            stage_name=f"{self.config.stage_name}_eof",
            worker_id=self.config.worker_id,
        )

    def start(self) -> None:
        for route in self.config.routes:
            route.exchange = MessageMiddlewareExchangeRabbitMQ(
                self.config.mom_host, route.output
            )
            logging.info("Route %s -> exchange %s", route.name, route.output)

        self.eof_coordinator = EofCoordinator(
            expected_eofs=self.config.expected_eofs,
            on_flush=self._on_flush,
            state_manager=self.eof_state_manager,
        )

        routing_keys = [f"worker_{self.config.worker_id}", "eof_broadcast"]
        self.input_mw = MessageMiddlewareExchangeRabbitMQ(
            host=self.config.mom_host,
            exchange_name=self.config.input_exchange,
            routing_keys=routing_keys,
            heartbeat=CONSUMER_HEARTBEAT,  # consumer: RabbitMQ detecta caidas y re-encola
        )

        logging.info(
            "Router listening on %s with keys %s",
            self.config.input_exchange,
            routing_keys,
        )
        self.input_mw.start_consuming(self._on_message)

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
                self._route_batch(client_id, batch)
            ack()
        except Exception:
            logging.exception("Error processing message; nack")
            nack()

    def _route_batch(self, client_id: str, batch: list) -> None:
        for route in self.config.routes:
            sub_batch = [tx for tx in batch if route.matches(tx)]
            if not sub_batch:
                continue

            if route.routing_strategy != "round_robin":
                raise ValueError(
                    f"Unsupported routing strategy: {route.routing_strategy}"
                )

            out_msg = serialize(
                build_raw_transactions_message(
                    client=client_id,
                    msg_id=str(uuid.uuid4()),  # TODO: cambiar msg id a incremental
                    batch=sub_batch,
                )
            )
            routing_key = route.next_routing_key()
            route.exchange.send(out_msg, routing_key=routing_key)
            logging.info(
                "Route %s: sent %d txs to %s (key=%s)",
                route.name,
                len(sub_batch),
                route.output,
                routing_key,
            )

    # en flush mando eofs
    def _on_flush(self, client_id: str) -> None:
        eof_msg = serialize(
            build_eof_message(client=client_id, msg_id=str(uuid.uuid4()))
        )
        for route in self.config.routes:
            route.exchange.send(eof_msg, routing_key="eof_broadcast")
            logging.info("EOF broadcast to %s for client %s", route.output, client_id)

    def stop(self) -> None:
        if self.input_mw:
            self.input_mw.stop_consuming()
            self.input_mw.close()
        for route in self.config.routes:
            if route.exchange:
                route.exchange.close()


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    router = TransactionRouter(config)

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        router.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    router.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
