import logging
import os
import signal
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src.common.middleware import (MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ)
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
    DateRangeRoute,
    ShardConfig,
    HistoricalAverageFilterStrategy,
)


CONFIG_PATH = "./config.yaml"
DATETIME_FORMAT = "%Y/%m/%d %H:%M"
TYPE = 0
NAME = 1


@dataclass
class FilterConfig:
    mom_host: str
    input_queue: str
    # TODO multi-output queue (D3): filter_usd se reusa por Q2, Q3 y Q5, que
    # consumen de colas distintas (q2_queue, q5_queue, date_filter_queue).
    # Para soportar fanout, output_queue tiene que pasar a List[str] y
    # FilterService debe publicar a todas. Por ahora single-output (Q1).
    output_queues: List[Tuple[str, str]]
    log_level: str
    strategy: FilterStrategy
    projection_fields: Optional[List[str]] = None
    control_queue: Optional[str] = None


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
        return CurrencyStrategy(output_queue=params["output_queue"], target_currency=params["target_currency"])

    if strategy_type == "amount_less_than":
        return AmountLessThanStrategy(output_queue=params["output_queue"], threshold=float(params["threshold"]))

    if strategy_type == "date_routing":
        raw_routes = params.get("routes", [])
        routes: List[DateRangeRoute] = []
        for raw_route in raw_routes:
            raw_match = raw_route["match"]
            raw_shard = raw_route.get("shard")
            shard_config = None

            if raw_shard is not None:
                shard_config = ShardConfig(by=raw_shard["by"], shards=int(raw_shard["shards"]))

            routes.append(
                DateRangeRoute(
                    from_date=datetime.strptime(
                        raw_match["from"],
                        DATETIME_FORMAT,
                    ),
                    to_date=datetime.strptime(
                        raw_match["to"],
                        DATETIME_FORMAT,
                    ),
                    queue=raw_route["output"], shard=shard_config))
        return DateStrategy(routes=routes)

    if strategy_type == "historical_average":
        # params: output_queue, control_queue, threshold
        out = params.get("output_queue")
        thresh = params.get("threshold", 0.01)
        return HistoricalAverageFilterStrategy(output_queue=out, threshold_multiplier=thresh)

    return NoStrategy("")

def _parse_projection_config(raw_projection: Dict[str, Any]) -> Optional[List[str]]:
    fields = raw_projection.get("fields")
    if not fields:
        return None
    return list(fields)


def _parse_output_queues(raw_value: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    result: List[Tuple[str, str]] = []
    if not raw_value:
        return result

    for raw_output in raw_value:
        output_type = raw_output["type"]
        output_name = raw_output["name"]
        shards = int(raw_output.get("shards", 1))
        if shards == 1:
            result.append((output_type, output_name))
            continue
        for shard_id in range(shards):
            result.append((output_type, f"{output_name}_shard_{shard_id}"))

    return result


def init_config() -> FilterConfig:
    file_config = _load_file_config()
    raw_strategy = file_config.get("strategy", {})
    raw_projection = file_config.get("projection", {})
    raw_inputs = file_config.get("inputs", [])
    raw_outputs = file_config.get("outputs", [])
    input_queue = ""

    if raw_inputs:
        input_queue = raw_inputs[0]["name"]
    strategy_params = raw_strategy.get("params", {})
    control_queue = os.getenv("CONTROL_QUEUE", strategy_params.get("control_queue", None))

    return FilterConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", input_queue),
        output_queues=_parse_output_queues(raw_outputs),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy),
        projection_fields=_parse_projection_config(raw_projection),
        control_queue=control_queue,
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

    # TODO: EOF propagation: por ahora se ignora cualquier mensaje que no sea
    # raw_transactions (incluido EOF). Esto es OK mientras downstream no
    # necesita saber cuándo termina el stream pero tiene que resolverse a futuro.
    if decoded["type"] != "raw_transactions":
        return None

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

        result[queue_name] = serialize(new_msg)

    return result

class FilterService:
    def __init__(self, config: FilterConfig) -> None:
        self.mom_host = config.mom_host
        self.input_queue = config.input_queue
        self.output_queues = config.output_queues
        self.strategy = config.strategy
        self.projection_fields = config.projection_fields
        self._input_middleware: Optional[MessageMiddlewareQueueRabbitMQ] = None
        self._output_middleware: Dict[str, Any] = {}
        self._running = False

    def start(self) -> None:
        logging.info("Starting filter service")
        self._running = True
        #TODO: Instanciar Exchange o Queue dependiendo de los nombres que llegan del config
        self._input_middleware = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.input_queue)
        for queue_data in self.output_queues:
            if queue_data[TYPE] == "exchange":
                self._output_middleware[queue_data[NAME]] = MessageMiddlewareExchangeRabbitMQ(self.mom_host, queue_data[NAME])
            else:
                self._output_middleware[queue_data[NAME]] = MessageMiddlewareQueueRabbitMQ(self.mom_host, queue_data[NAME])

        def on_message(message, ack, nack):
            try:
                result = process_message(message, self.strategy, self.projection_fields)
                if result is not None:
                    for queue, message in result.items():
                        self._output_middleware[queue].send(message)
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
