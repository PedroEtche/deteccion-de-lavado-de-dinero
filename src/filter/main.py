import logging
import os
import signal
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml

from src.common.middleware import MessageMiddlewareQueueRabbitMQ
from src.communication.protocols.queue_protocol.internal import (
    TransactionRow,
    build_eof_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)

from .strategies import (
    FieldLessThanStrategy,
    CurrencyStrategy,
    DateStrategy,
    DateRangeRoute,
    FilterStrategy,
    NoStrategy,
    DateRangeRoute,
    FieldGreaterThanStrategy,
    PaymentFormatStrategy,
)


CONFIG_PATH = "./config.yaml"
DATETIME_FORMAT = "%Y/%m/%d %H:%M"


@dataclass
class FilterConfig:
    mom_host: str
    input_queue: str
    output_queues: List[str]
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


def _parse_strategy_config(
    raw_strategy: Dict[str, Any], output_queues: List[str]
) -> FilterStrategy:
    strategy_type = raw_strategy.get("type", "noop")
    params = raw_strategy.get("params", {})
    # Estrategias single-output (currency, amount_less_than, no-op) usan la
    # primera cola configurada. `date` lleva su propia tabla de routes.
    default_output = output_queues[0] if output_queues else ""

    if strategy_type == "currency":
        return CurrencyStrategy(default_output, params["target_currency"])

    if strategy_type == "field_less_than":
        return FieldLessThanStrategy(params["output_queue"], params["field_name"],float(params["threshold"]))

    if strategy_type == "field_greater_than":
        return FieldGreaterThanStrategy(params["output_queue"], params["field_name"], float(params["threshold"]))

    if strategy_type == "payment_format":
        return PaymentFormatStrategy(default_output, params.get("formats", []))

    if strategy_type == "date":
        raw_routes = params.get("routes", [])
        routes: List[DateRangeRoute] = []
        for raw_route in raw_routes:
            routes.append(
                DateRangeRoute(
                    from_date=datetime.strptime(
                        raw_route["from"],
                        DATETIME_FORMAT,
                    ),
                    to_date=datetime.strptime(
                        raw_route["to"],
                        DATETIME_FORMAT,
                    ),
                    queue=raw_route["queue"],
                )
            )

        return DateStrategy(routes=routes)

    return NoStrategy(default_output)

def _parse_projection_config(raw_projection: Dict[str, Any]) -> Optional[List[str]]:
    fields = raw_projection.get("fields")
    if not fields:
        return None
    return list(fields)

def _parse_output_queues(raw_value: str) -> List[str]:
    if not raw_value:
        return []

    return [
        queue.strip()
        for queue in raw_value.split(",")
        if queue.strip()
    ]

def init_config() -> FilterConfig:
    file_config = _load_file_config()
    raw_strategy = file_config.get("strategy", {})
    raw_projection = file_config.get("projection", {})
    raw_output_queues = os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", ""))

    output_queues = _parse_output_queues(raw_output_queues)

    return FilterConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queues=output_queues,
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy, output_queues),
        projection_fields=_parse_projection_config(raw_projection),
    )


def log_config(config: FilterConfig) -> None:
    logging.info(
        "Filter startup with: mom_host=%s | input_queue=%s | output_queue=%s | strategy=%s | projection_fields=%s",
        config.mom_host,
        config.input_queue,
        config.output_queues,
        str(config.strategy),
        config.projection_fields,
    )


def _project_row(row: TransactionRow, fields: List[str]) -> TransactionRow:
    return TransactionRow(**{f: getattr(row, f) for f in fields})


def process_message(
    message_bytes: bytes,
    strategy: FilterStrategy,
    projection_fields: Optional[List[str]],
) -> Optional[Dict[str, bytes]]:
    decoded = deserialize(message_bytes)

    # if decoded["type"] != "raw_transactions":
    #     return None

    batch = decoded["payload"]["batch"]
    routed_batches = strategy.filter_batch(batch)
    result: Dict[str, bytes] = {}

    for queue_name, rows in routed_batches.items():
        if projection_fields:
            rows = [ _project_row(row, projection_fields) for row in rows ]

        if not rows:
            continue

        new_msg = build_raw_transactions_message(
            client=decoded["client"],
            msg_id=str(uuid.uuid4()),
            batch=rows,
        )
        logging.info("Filtered batch for queue %s", queue_name)
        result[queue_name] = serialize(new_msg)

    return result

class FilterService:
    def __init__(self, config: FilterConfig) -> None:
        self.mom_host = config.mom_host
        self.strategy = config.strategy
        self.projection_fields = config.projection_fields
        self.input_queue = config.input_queue
        self.output_queues = config.output_queues
        self._input_middleware: Optional[MessageMiddlewareQueueRabbitMQ] = None
        self._output_middleware: Dict[str, MessageMiddlewareQueueRabbitMQ] = {}
        self._running = False

    def start(self) -> None:
        logging.info("Starting filter service")
        self._running = True
        self._input_middleware = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.input_queue)
        for queue_name in self.output_queues:
            logging.info("Initializing output middleware for queue: %s", queue_name)
            self._output_middleware[queue_name] = MessageMiddlewareQueueRabbitMQ(self.mom_host, queue_name)

        def on_message(message, ack, nack):
            try:
                decoded = deserialize(message)
                if decoded["type"] == "eof":
                    self._forward_eof(decoded["client"])
                    ack()
                    return
                result = process_message(message, self.strategy, self.projection_fields)
                if result is not None:
                    for queue, message in result.items():
                        logging.info("Sending message to queue %s", queue)
                        self._output_middleware[queue].send(message)
                ack()
            except Exception:
                logging.exception("error processing message")
                nack()

        try:
            self._input_middleware.start_consuming(on_message)
        finally:
            self._close_middlewares()

    def _forward_eof(self, client: str) -> None:
        """Reenvía un EOF a todas las colas de salida con msg_id nuevo."""
        for queue_name, mw in self._output_middleware.items():
            eof = build_eof_message(client=client, msg_id=str(uuid.uuid4()))
            logging.info("Forwarding EOF for client %s to queue %s", client, queue_name)
            mw.send(serialize(eof))

    def stop(self) -> None:
        logging.info("Stopping filter service")
        self._running = False
        if self._input_middleware is not None:
            try:
                self._input_middleware.stop_consuming()
            except Exception:
                logging.exception("error stopping consumer")

    def _close_middlewares(self) -> None:
        if self._input_middleware is not None:
            try:
                self._input_middleware.close()
            except Exception:
                logging.exception("error closing input middleware")

        for mw in self._output_middleware.values():
            try:
                mw.close()
            except Exception:
                logging.exception("error closing output middleware")


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
