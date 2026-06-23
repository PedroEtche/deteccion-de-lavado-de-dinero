import logging
import os
from dataclasses import dataclass
from typing import Any, List

from src.common.communication.internal import (
    build_eof_message,
    build_raw_transactions_message,
    serialize,
)
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.common.utils import load_yaml_config
from src.common.worker import StreamWorker, run_worker

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
    # Lo lee BaseWorker.__init__ (self.strategy = config.strategy); el router no
    # usa strategy, asi que queda en None.
    strategy: Any = None


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


class TransactionRouter(StreamWorker):
    """
    Consumes raw transactions from the gateway, evaluates each tx against
    every configured route, and emits one sub-batch per matching route to
    that route's output exchange.

    Hereda de StreamWorker: la infra de consumo/EOF/identidad la pone la base;
    aca solo van los hooks (setup_outputs/close_outputs/handle_data/on_flush) y
    el ruteo propio. Usa el input_routing_keys por defecto (worker_id + eof).
    """

    def setup_outputs(self) -> None:
        for route in self.config.routes:
            route.exchange = MessageMiddlewareExchangeRabbitMQ(
                self.config.mom_host, route.output
            )
            logging.info("Route %s -> exchange %s", route.name, route.output)

    def close_outputs(self) -> None:
        for route in self.config.routes:
            if route.exchange:
                route.exchange.close()

    def handle_data(self, client_id: str, msg_type: str, batch: list) -> None:
        self._route_batch(client_id, batch)

    def _route_batch(self, client_id: str, batch: list) -> None:
        for route in self.config.routes:
            sub_batch = [tx for tx in batch if route.matches(tx)]
            if not sub_batch:
                continue

            if route.routing_strategy != "round_robin":
                raise ValueError(
                    f"Unsupported routing strategy: {route.routing_strategy}"
                )

            msg_id = self._next_msg_id()
            out_msg = serialize(
                build_raw_transactions_message(
                    client=client_id,
                    msg_id=msg_id,
                    batch=sub_batch,
                    sender=self.sender_id,
                )
            )
            routing_key = route.routing_key_for(msg_id)
            route.exchange.send(out_msg, routing_key=routing_key)
            logging.info(
                "Route %s: sent %d txs to %s (key=%s)",
                route.name,
                len(sub_batch),
                route.output,
                routing_key,
            )

    # en flush mando eofs
    def on_flush(self, client_id: str) -> None:
        eof_msg = serialize(
            build_eof_message(
                client=client_id, msg_id=self._next_msg_id(), sender=self.sender_id
            )
        )
        for route in self.config.routes:
            route.exchange.send(eof_msg, routing_key="eof_broadcast")
            logging.info("EOF broadcast to %s for client %s", route.output, client_id)


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    return run_worker(TransactionRouter(config))


if __name__ == "__main__":
    raise SystemExit(main())
