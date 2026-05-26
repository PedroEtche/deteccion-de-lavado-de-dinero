import logging
import os
import signal
import uuid
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional

import yaml

from src.common.middleware import MessageMiddlewareQueueRabbitMQ
from src.communication.protocols.queue_protocol.internal import (
    TransactionRow,
    build_raw_transactions_message,
    deserialize,
    serialize,
)

from .strategies import (
    AmountLessThanStrategy,
    CurrencyStrategy,
    DateStrategy,
    FilterStrategy,
    NoStrategy,
)


CONFIG_PATH = "./config.yaml"


@dataclass
class FilterConfig:
    mom_host: str
    input_queue: str
    # TODO multi-output queue (D3): filter_usd se reusa por Q2, Q3 y Q5, que
    # consumen de colas distintas (q2_queue, q5_queue, date_filter_queue).
    # Para soportar fanout, output_queue tiene que pasar a List[str] y
    # FilterService debe publicar a todas. Por ahora single-output (Q1).
    output_queue: str
    log_level: str
    strategy: FilterStrategy
    projection_fields: Optional[List[str]] = None


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def _parse_strategy_config(raw_strategy: Dict[str, Any]) -> FilterStrategy:
    strategy_type = raw_strategy.get("type", "noop")
    params = raw_strategy.get("params", {})

    if strategy_type == "currency":
        return CurrencyStrategy(params["target_currency"])

    if strategy_type == "amount_less_than":
        return AmountLessThanStrategy(float(params["threshold"]))

    if strategy_type == "date":
        return DateStrategy(
            from_date=date.fromisoformat(str(params["from"])),
            to_date=date.fromisoformat(str(params["to"]))
        )

    return NoStrategy()


def _parse_projection_config(raw_projection: Dict[str, Any]) -> Optional[List[str]]:
    fields = raw_projection.get("fields")
    if not fields:
        return None
    return list(fields)


def init_config() -> FilterConfig:
    file_config = _load_file_config()
    raw_strategy = file_config.get("strategy", {})
    raw_projection = file_config.get("projection", {})

    return FilterConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy),
        projection_fields=_parse_projection_config(raw_projection),
    )


def log_config(config: FilterConfig) -> None:
    logging.info(
        "Filter startup with: mom_host=%s | input_queue=%s | output_queue=%s | strategy=%s | projection_fields=%s",
        config.mom_host,
        config.input_queue,
        config.output_queue,
        str(config.strategy),
        config.projection_fields,
    )


def _project_row(row: TransactionRow, fields: List[str]) -> TransactionRow:
    return TransactionRow(**{f: getattr(row, f) for f in fields})


def process_message(
    message_bytes: bytes,
    strategy: FilterStrategy,
    projection_fields: Optional[List[str]],
) -> Optional[bytes]:
    decoded = deserialize(message_bytes)
    # TODO EOF propagation: por ahora se ignora cualquier mensaje que no sea
    # raw_transactions (incluido EOF). Esto es OK mientras downstream no
    # necesita saber cuándo termina el stream pero tiene que resolverse a futuro.
    if decoded["type"] != "raw_transactions":
        return None

    batch = decoded["payload"]["batch"]
    filtered = strategy.filter_batch(batch)

    if projection_fields:
        filtered = [_project_row(row, projection_fields) for row in filtered]

    if not filtered:
        return None

    new_msg = build_raw_transactions_message(
        client=decoded["client"],
        msg_id=str(uuid.uuid4()),
        batch=filtered,
    )
    return serialize(new_msg)


class FilterService:
    def __init__(self, config: FilterConfig) -> None:
        self.mom_host = config.mom_host
        self.input_queue = config.input_queue
        self.output_queue = config.output_queue
        self.strategy = config.strategy
        self.projection_fields = config.projection_fields
        self._input_middleware: Optional[MessageMiddlewareQueueRabbitMQ] = None
        self._output_middleware: Optional[MessageMiddlewareQueueRabbitMQ] = None
        self._running = False

    def start(self) -> None:
        logging.info("Starting filter service")
        self._running = True
        self._input_middleware = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.input_queue)
        self._output_middleware = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.output_queue)

        def on_message(message, ack, nack):
            try:
                result = process_message(message, self.strategy, self.projection_fields)
                if result is not None:
                    self._output_middleware.send(result)
                ack()
            except Exception:
                logging.exception("error processing message")
                nack()

        try:
            self._input_middleware.start_consuming(on_message)
        finally:
            self._close_middlewares()

    def stop(self) -> None:
        logging.info("Stopping filter service")
        self._running = False
        if self._input_middleware is not None:
            try:
                self._input_middleware.stop_consuming()
            except Exception:
                logging.exception("error stopping consumer")

    def _close_middlewares(self) -> None:
        for mw in (self._input_middleware, self._output_middleware):
            if mw is None:
                continue
            try:
                mw.close()
            except Exception:
                logging.exception("error closing middleware")


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
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
