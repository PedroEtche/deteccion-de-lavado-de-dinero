import logging
import os
import signal
import uuid
from dataclasses import dataclass
from typing import Any, Dict

import yaml

from src.common import middleware
from src.common.eof import EofCoordinator
from src.communication.protocols.queue_protocol.internal import (
    build_batch_message,
    build_eof_message,
    deserialize,
    serialize,
)

from .strategies import (
    AccountPairCountStategy,
    BankMaxAmountStrategy,
    GroupStrategy,
    NoStrategy,
    PaymentFormatAverageStrategy,
    AccountStrategy,
    MergeRoutingStrategy,
)

CONFIG_PATH = "./config.yaml"


@dataclass
class GroupConfig:
    mom_host: str
    input_queue: str
    output_exchange: str
    log_level: str
    eof_fanout: str
    expected_eofs: int
    strategy: GroupStrategy

def _parse_strategy_config(raw_strategy: str) -> GroupStrategy:
    strategy_type = raw_strategy.get("type", "noop")
    params = raw_strategy.get("params", {})

    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy(params["base_routing_key"], params["shard_amount"])

    if strategy_type == "PaymentFormatAverage":
        return PaymentFormatAverageStrategy(params["base_routing_key"], params["shard_amount"])
    
    if strategy_type == "AccountPairCount":
        return AccountPairCountStategy(params["base_routing_key"], params["shard_amount"])
    
    if strategy_type == "MergeRouting":
        return MergeRoutingStrategy(params["base_routing_key"], params["shard_amount"])

    return NoStrategy(params["base_routing_key"])


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def init_config() -> GroupConfig:
    file_config = _load_file_config()
    raw_strategy = file_config.get("strategy", "NoStrategy")

    return GroupConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_exchange=os.getenv("OUTPUT_EXCHANGE", file_config.get("output_exchange", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        eof_fanout=os.getenv("EOF_FANOUT", file_config.get("eof_fanout", "")),
        expected_eofs=int(os.getenv("EXPECTED_EOFS", file_config.get("expected_eofs", "1"))),
        strategy=_parse_strategy_config(raw_strategy),
    )


def log_config(config: GroupConfig) -> None:
    logging.info(
        "Group startup with: mom_host=%s | input_queue=%s | output_exchange=%s | "
        "eof_fanout=%s | expected_eofs=%d | strategy=%s",
        config.mom_host,
        config.input_queue,
        config.output_exchange,
        config.eof_fanout,
        config.expected_eofs,
        config.strategy,
    )


class GroupService:
    """Group por batch (stateless cross-batch). Emite EOF a cada route luego de
    contar `expected_eofs` desde upstream."""

    def __init__(self, config: GroupConfig) -> None:
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            config.mom_host, config.input_queue
        )
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=config.mom_host, exchange_name=config.output_exchange
        )
        self.strategy = config.strategy
        self.coord = EofCoordinator(
            mom_host=config.mom_host,
            fanout_name=config.eof_fanout,
            expected_eofs=config.expected_eofs,
            on_flush=self._flush_client,
        )

    def start(self) -> None:
        self.coord.start()
        self.input_queue.start_consuming(self._on_input)

    def stop(self) -> None:
        logging.info("Stopping group service")
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.exception("error stopping input consumer")
        self.coord.stop(timeout=10)

    def close(self) -> None:
        for closeable in (self.input_queue, self.output_exchange):
            try:
                closeable.close()
            except Exception:
                logging.exception("error closing middleware")
        self.coord.close()

    def _on_input(self, message, ack, _nack):
        decoded = deserialize(message)
        client = decoded["client"]
        if decoded["type"] == "eof":
            logging.info("Received EOF from upstream for client %s", client)
            self.coord.broadcast(client)
        else:
            with self.coord.lock():
                logging.debug("Processing data message for client %s", client)
                routed = self.strategy.group_and_route(decoded["payload"]["batch"])
                for route, grouped in routed:
                    if not grouped:
                        continue
                    batch_msg = build_batch_message(
                        message_type="batch",
                        client=client,
                        msg_id=str(uuid.uuid4()),
                        batch=grouped,
                    )
                    self.output_exchange.send(serialize(batch_msg), routing_key=route)
        ack()

    def _flush_client(self, client: str) -> None:
        """Bajo `coord.lock()`. No hay estado por cliente: solo propaga el EOF."""
        for route in self.strategy.get_eof_routes():
            eof_msg = build_eof_message(client=client, msg_id=str(uuid.uuid4()))
            self.output_exchange.send(serialize(eof_msg), routing_key=route)


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    service = GroupService(config)

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    try:
        service.start()
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
