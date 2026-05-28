import logging
import os
import signal
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

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
    AccountStrategy,
    AggregatorStrategy,
    BankMaxAmountStrategy,
    CountStrategy,
    NoStrategy,
    PaymentFormatAverageStrategy,
)

CONFIG_PATH = "./config.yaml"


@dataclass
class AggregatorConfig:
    mom_host: str
    input_queue: str
    shard_id: str
    base_routing_key: str
    output_queue: str
    log_level: str
    eof_fanout: str
    expected_eofs: int
    strategy: AggregatorStrategy
    # Si está seteado, el aggregator se bindea al exchange con routing_key
    # igual a `input_queue` (en lugar de consumir de una cola directa). Necesario
    # cuando el upstream publica a un exchange direct con sharding por route.
    input_exchange: Optional[str] = None

def _extract_strategy_type(raw_strategy) -> str:
    if isinstance(raw_strategy, dict):
        return str(raw_strategy.get("type", "NoStrategy"))
    return str(raw_strategy or "NoStrategy")


def _parse_strategy_config(raw_strategy) -> AggregatorStrategy:
    strategy_type = _extract_strategy_type(raw_strategy)

    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy()

    if strategy_type == "AccountPairCount":
        return AccountPairCountStategy()

    if strategy_type in ("PaymentFormatAverage", "MergeRouting"):
        return PaymentFormatAverageStrategy()

    if strategy_type == "Account":
        return AccountStrategy()

    if strategy_type == "Count":
        return CountStrategy()

    return NoStrategy()


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def init_config() -> AggregatorConfig:
    file_config = _load_file_config()
    raw_strategy = os.getenv("STRATEGY", file_config.get("strategy", "NoStrategy"))

    input_exchange = os.getenv("INPUT_EXCHANGE", file_config.get("input_exchange", ""))

    return AggregatorConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")), # <--- ADD THIS LINE
        shard_id=os.getenv("SHARD_ID", file_config.get("shard_id", "")),
        base_routing_key=os.getenv("BASE_ROUTING_KEY", file_config.get("base_routing_key", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        eof_fanout=os.getenv("EOF_FANOUT", file_config.get("eof_fanout", "")),
        expected_eofs=int(os.getenv("EXPECTED_EOFS", file_config.get("expected_eofs", "1"))),
        strategy=_parse_strategy_config(raw_strategy),
        input_exchange=input_exchange if input_exchange else None,
    )


def log_config(config: AggregatorConfig) -> None:
    logging.info(
        "Aggregator startup with: mom_host=%s | input_queue=%s | output_queue=%s | "
        "eof_fanout=%s | expected_eofs=%d | strategy=%s",
        config.mom_host,
        config.input_exchange,
        config.output_queue,
        config.eof_fanout,
        config.expected_eofs,
        config.strategy,
    )


class AggregatorService:
    """Stateful aggregator. Cuenta `expected_eofs` antes de flushear por cliente."""

    def __init__(self, config: AggregatorConfig) -> None:
        if config.input_exchange:
            self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                host=config.mom_host,
                exchange_name=config.input_exchange,
                routing_keys=[config.input_queue],
            )
        else:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                config.mom_host, config.input_queue
            )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            config.mom_host, config.output_queue
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
        self._running = True
        self.input_queue.start_consuming(self._on_input)

    def stop(self) -> None:
        logging.info("Stopping aggregator service")
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.exception("error stopping input consumer")
        self.coord.stop(timeout=10)

    def close(self) -> None:
        for closeable in (self.input_queue, self.output_queue):
            try:
                closeable.close()
            except Exception:
                logging.exception("error closing middleware")
        self.coord.close()

    def _on_input(self, message, ack, _nack):
        decoded = deserialize(message)
        logging.info("Received message of type %s for client %s", decoded["type"], decoded["client"])
        client = decoded["client"]
        if decoded["type"] == "eof":
            logging.info("Received EOF from upstream for client %s", client)
            self.coord.broadcast(client)
        else:
            with self.coord.lock():
                logging.debug("Processing data message for client %s", client)
                self.strategy.aggregate_batch(decoded["payload"]["batch"], client)

    def _flush_client(self, client: str) -> None:
        """Se invoca bajo `coord.lock()` cuando llegaron `expected_eofs` EOFs."""
        logging.info("Flushing aggregated result for client %s", client)
        batch = self.strategy.get_result_for_client(client)
        self.output_queue.send(
            serialize(
                build_batch_message(
                    message_type="batch",
                    client=client,
                    msg_id=str(uuid.uuid4()),
                    batch=batch,
                )
            )
        )
        self.strategy.clear_client_state(client)

        self.output_queue.send(
            serialize(build_eof_message(client=client, msg_id=str(uuid.uuid4())))
        )


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    service = AggregatorService(config)

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
