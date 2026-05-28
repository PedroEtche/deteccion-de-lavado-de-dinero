import logging
import os
import signal
import struct
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, IO, List, Optional, Set, Tuple

import yaml

from src.common.middleware import (MessageMiddlewareQueueRabbitMQ, MessageMiddlewareExchangeRabbitMQ)
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
    HistoricalAverageFilterStrategy,
    NoStrategy,
    DateRangeRoute,
    OriginNotEqualDestinationStrategy,
    ShardConfig,
)


CONFIG_PATH = "./config.yaml"
DATETIME_FORMAT = "%Y/%m/%d %H:%M"
TYPE = 0
NAME = 1


@dataclass
class FilterConfig:
    mom_host: str
    input_queue: str
    output_queues: List[str]
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


def _parse_strategy_config(
    raw_strategy: Dict[str, Any], output_queues: List[str]
) -> FilterStrategy:
    strategy_type = raw_strategy.get("type", "noop")
    params = raw_strategy.get("params", {})
    # Estrategias single-output (currency, amount_less_than, no-op) usan la
    # primera cola configurada. `date` lleva su propia tabla de routes.
    # output_queues contiene tuplas (type, name); extraemos solo el nombre.
    default_output = output_queues[0][NAME] if output_queues else ""

    if strategy_type == "currency":
        return CurrencyStrategy(default_output, params["target_currency"])

    if strategy_type == "field_less_than":
        return FieldLessThanStrategy(params["output_queue"], params["field_name"],float(params["threshold"]))

    if strategy_type == "field_greater_than":
        return FieldGreaterThanStrategy(params["output_queue"], params["field_name"], float(params["threshold"]))

    if strategy_type == "payment_format":
        return PaymentFormatStrategy(default_output, params.get("formats", []))

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
        thresh = params.get("threshold", 0.01)
        return HistoricalAverageFilterStrategy(output_queue=default_output, threshold_multiplier=thresh)

    return NoStrategy(default_output)

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

    output_queues = _parse_output_queues(raw_outputs)

    return FilterConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", input_queue or file_config.get("input_queue", "")),
        output_queues=output_queues,
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy, output_queues),
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
        self.control_queue = config.control_queue
        self._input_middleware: Optional[MessageMiddlewareQueueRabbitMQ] = None
        self._control_middleware: Optional[MessageMiddlewareQueueRabbitMQ] = None
        self._output_middleware: Dict[str, Any] = {}
        self._running = False
        # Disk buffer for HistoricalAverageFilterStrategy: messages that arrive
        # before the historical averages are ready are spilled to temp files so
        # memory is not exhausted on large datasets.
        self._buffer_files: Dict[str, IO[bytes]] = {}
        self._pending_eofs: Set[str] = set()

    def start(self) -> None:
        logging.info("Starting filter service")
        self._running = True
        self._input_middleware = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.input_queue)
        for queue_data in self.output_queues:
            if queue_data[TYPE] == "exchange":
                self._output_middleware[queue_data[NAME]] = MessageMiddlewareExchangeRabbitMQ(self.mom_host, queue_data[NAME])
            else:
                self._output_middleware[queue_data[NAME]] = MessageMiddlewareQueueRabbitMQ(self.mom_host, queue_data[NAME])

        if self.control_queue and isinstance(self.strategy, HistoricalAverageFilterStrategy):
            self._control_middleware = MessageMiddlewareQueueRabbitMQ(self.mom_host, self.control_queue)
            control_thread = threading.Thread(
                target=self._consume_control,
                daemon=True,
                name="filter-control-consumer",
            )
            control_thread.start()

        try:
            self._input_middleware.start_consuming(self._on_data_message)
        finally:
            self._close_middlewares()

    def _consume_control(self) -> None:
        try:
            self._control_middleware.start_consuming(self._on_control_message)
        except Exception:
            logging.exception("error in control consumer thread")

    def _on_control_message(self, message: bytes, ack, nack) -> None:
        try:
            decoded = deserialize(message)
            client = decoded["client"]
            msg_type = decoded.get("type")
            if msg_type in ("joined_data", "batch") and isinstance(self.strategy, HistoricalAverageFilterStrategy):
                batch = decoded["payload"]["batch"]
                self.strategy.update_averages(client, batch)
                logging.info("Averages received for client %s, scheduling buffer flush", client)
                # Schedule flush on the main pika event loop so output middlewares
                # are only used from a single thread.
                if self._input_middleware is not None:
                    self._input_middleware.connection.add_callback_threadsafe(
                        lambda c=client: self._flush_client_buffer(c)
                    )
            ack()
        except Exception:
            logging.exception("error processing control message")
            nack()

    def _on_data_message(self, message: bytes, ack, nack) -> None:
        try:
            decoded = deserialize(message)
            client = decoded["client"]

            if decoded["type"] == "eof":
                if self._averages_ready(client):
                    self._forward_eof(client)
                else:
                    logging.info("Data EOF received but no averages yet for client %s, buffering", client)
                    self._pending_eofs.add(client)
                ack()
                return

            if isinstance(self.strategy, HistoricalAverageFilterStrategy):
                if not self._averages_ready(client):
                    self._write_to_disk(client, message)
                    ack()
                    return
                self.strategy._current_client = client

            result = process_message(message, self.strategy, self.projection_fields)
            if result is not None:
                for queue, out_msg in result.items():
                    self._output_middleware[queue].send(out_msg)
            ack()
        except Exception:
            logging.exception("error processing data message")
            nack()

    def _averages_ready(self, client: str) -> bool:
        if not isinstance(self.strategy, HistoricalAverageFilterStrategy):
            return True
        return bool(self.strategy.averages_by_client.get(client))

    def _write_to_disk(self, client: str, message: bytes) -> None:
        if client not in self._buffer_files:
            self._buffer_files[client] = tempfile.TemporaryFile()
        f = self._buffer_files[client]
        f.write(struct.pack(">I", len(message)))
        f.write(message)

    def _flush_client_buffer(self, client: str) -> None:
        """Called in the main pika event loop via add_callback_threadsafe."""
        self.strategy._current_client = client
        f = self._buffer_files.pop(client, None)
        count = 0
        if f is not None:
            f.flush()
            f.seek(0)
            while True:
                hdr = f.read(4)
                if len(hdr) < 4:
                    break
                (length,) = struct.unpack(">I", hdr)
                buffered_msg = f.read(length)
                if len(buffered_msg) < length:
                    break
                count += 1
                result = process_message(buffered_msg, self.strategy, self.projection_fields)
                if result is not None:
                    for queue, out_msg in result.items():
                        self._output_middleware[queue].send(out_msg)
            f.close()
        logging.info("Flushed %d buffered messages from disk for client %s", count, client)
        if client in self._pending_eofs:
            self._pending_eofs.discard(client)
            self._forward_eof(client)

    def _forward_eof(self, client: str) -> None:
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
        for f in self._buffer_files.values():
            try:
                f.close()
            except Exception:
                pass
        self._buffer_files.clear()

        if self._input_middleware is not None:
            try:
                self._input_middleware.close()
            except Exception:
                logging.exception("error closing input middleware")

        if self._control_middleware is not None:
            try:
                self._control_middleware.close()
            except Exception:
                logging.exception("error closing control middleware")

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
