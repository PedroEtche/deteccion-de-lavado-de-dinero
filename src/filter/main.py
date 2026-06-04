import logging
import os
import signal
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, IO, List, Optional, Set, Tuple

import yaml

from src.common.middleware import (
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
)
from src.common.communication.internal import (
    TransactionRow,
    build_batch_message,
    build_raw_transactions_message,
    build_eof_message,
    deserialize,
    serialize,
)

from .strategies import (
    AmountComparisonStrategy,
    CurrencyStrategy,
    FilterStrategy,
    NoStrategy,
)


CONFIG_PATH = "./config.yaml"
DATETIME_FORMAT = "%Y/%m/%d %H:%M"
TYPE = 0
NAME = 1


@dataclass
class FilterConfig:
    mom_host: str
    input_queue: str
    output_queue: str
    log_level: str
    strategy: FilterStrategy


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def _build_strategy(strategy_data: List[Dict[str, Any]]) -> FilterStrategy:
    params: Dict[str, Any] = {}
    for item in strategy_data:
        params.update(item)

    strategy_type = params.get("type", "none")

    _BUILDERS = {
        "none": lambda p: NoStrategy(),
        "currency": lambda p: CurrencyStrategy(p["value"]),
        "amount": lambda p: AmountComparisonStrategy(p["condition"], p["threshold"]),
    }

    builder = _BUILDERS.get(strategy_type)
    if builder is None:
        raise ValueError(f"Unknown strategy type: {strategy_type!r}")
    return builder(params)


def init_config() -> FilterConfig:
    data = _load_file_config()
    return FilterConfig(
        mom_host=data.get("mom_host", "rabbitmq"),
        input_queue=data.get("input", ""),
        output_queue=data.get("output", ""),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        strategy=_build_strategy(data.get("strategy", [])),
    )


def log_config(config: FilterConfig) -> None:
    logging.info(
        "Filter startup with: mom_host=%s | input_queue=%s | output_queue=%s | strategy=%s",
        config.mom_host,
        config.input_queue,
        config.output_queue,
        str(config.strategy),
    )


class FilterService:
    def __init__(self, config: FilterConfig) -> None:
        self.config = config
        self.strategy = config.strategy
        self._running = False

    def start(self) -> None:
        logging.info("Starting filter service")
        self._running = True
        self.input_queue = MessageMiddlewareQueueRabbitMQ(
            self.config.mom_host, self.config.input_queue
        )
        self.output_queue = MessageMiddlewareQueueRabbitMQ(
            self.config.mom_host, self.config.output_queue
        )

        self.input_queue.start_consuming(self._on_data_message)

    def _on_data_message(self, message: bytes, ack, nack) -> None:
        try:
            decoded = deserialize(message)
            msg_type = decoded.get("type")

            if msg_type == "eof":
                logging.info("Received EOF, forwarding downstream")
                ack()
                return

            if msg_type != "raw_transactions":
                logging.debug("Ignoring message of type %s", msg_type)
                ack()
                return

            batch = decoded["payload"]["batch"]
            filtered_batch = self.strategy.filter_batch(batch)
            logging.info(
                "Filtered batch: %d in -> %d out", len(batch), len(filtered_batch)
            )
            out_msg = serialize(
                build_raw_transactions_message(
                    client=decoded["client"],
                    msg_id=decoded["msg_id"],
                    batch=filtered_batch,
                )
            )
            self.output_queue.send(out_msg)
            ack()
        except Exception:
            logging.exception("error processing data message")
            nack()

    def _forward_eof(self, client: str) -> None:
        pass

    def stop(self) -> None:
        logging.info("Stopping filter service")
        self._running = False
        # if self._input_middleware is not None:
        #     try:
        #         self._input_middleware.stop_consuming()
        #     except Exception:
        #         logging.exception("error stopping consumer")


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    service = FilterService(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    service.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
