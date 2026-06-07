import logging
import os
import signal
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml

from src.common.worker import BaseWorker
from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.common.communication.internal import (
    build_eof_message,
    build_raw_transactions_message,
    serialize,
)

CONFIG_PATH = "./config.yaml"
_DATE_FORMATS = ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d", "%Y-%m-%d")


def _parse_date(value: str) -> datetime:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except (ValueError, TypeError):
            continue
    raise ValueError(f"Unrecognized date format: {value!r}")


@dataclass
class DateRoute:
    name: str
    output_exchange: str
    from_date: datetime
    to_date: datetime
    num_downstream_workers: int = 1
    routing_strategy: str = "round_robin"
    _exchange: Optional[MessageMiddlewareExchangeRabbitMQ] = field(
        default=None, init=False, repr=False
    )
    _next_worker: int = field(default=1, init=False, repr=False)

    def matches(self, dt: datetime) -> bool:
        return self.from_date <= dt <= self.to_date


@dataclass
class DateRouterConfig:
    mom_host: str
    input_exchange: str
    output_exchange: str
    log_level: str
    expected_eofs: int
    worker_id: int
    routes: List[DateRoute]
    num_downstream_workers: int = 1
    routing_strategy: str = "round_robin"
    strategy: Any = None


def _build_routes(raw_routes: List[Dict[str, Any]], default_workers: int, default_strategy: str) -> List[DateRoute]:
    if not raw_routes:
        raise ValueError("DateRouter requires at least one route")
    routes = []
    for item in raw_routes:
        routes.append(
            DateRoute(
                name=str(item["name"]),
                output_exchange=str(item["output"]),
                from_date=_parse_date(str(item["from_date"])),
                to_date=_parse_date(str(item["to_date"])),
                num_downstream_workers=int(item.get("num_downstream_workers", default_workers)),
                routing_strategy=str(item.get("routing_strategy", default_strategy)).lower(),
            )
        )
    return routes


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def init_config() -> DateRouterConfig:
    data = _load_file_config()
    default_workers = int(os.getenv("NUM_DOWNSTREAM_WORKERS", "1"))
    default_strategy = os.getenv("ROUTING_STRATEGY", "round_robin").lower()
    return DateRouterConfig(
        mom_host=data.get("mom_host", "rabbitmq"),
        input_exchange=data.get("input", ""),
        output_exchange="",
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        expected_eofs=int(os.getenv("EOF_EXPECTED", "1")),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        routes=_build_routes(data.get("routes", []), default_workers, default_strategy),
        num_downstream_workers=default_workers,
        routing_strategy=default_strategy,
    )


class DateRouterWorker(BaseWorker):
    """Stateless router. Each row is forwarded to every route whose date range matches.

    Rows with an unparseable timestamp or no matching route are dropped.
    A row matching N routes produces N independent sub-batches downstream.
    """

    def __init__(self, config: DateRouterConfig) -> None:
        super().__init__(config)
        self.routes: List[DateRoute] = list(config.routes)

    def setup_outputs(self) -> None:
        for route in self.routes:
            route._exchange = MessageMiddlewareExchangeRabbitMQ(
                self.config.mom_host, route.output_exchange
            )

    def process_data(self, client_id: str, msg_id: str, payload: dict) -> None:
        batch = payload.get("batch", [])
        if not batch:
            return

        buckets: Dict[str, List[Any]] = {route.name: [] for route in self.routes}
        for row in batch:
            row_date = getattr(row, "date", None)
            if row_date is None:
                continue
            for route in self.routes:
                if route.matches(row_date):
                    buckets[route.name].append(row)

        for route in self.routes:
            rows = buckets[route.name]
            if not rows:
                continue
            out_msg = build_raw_transactions_message(
                client=client_id,
                msg_id=str(uuid.uuid4()),
                batch=rows,
            )
            self._send_to_route(route, client_id, out_msg, shard_key=None)

    def flush_state(self, client_id: str) -> None:
        pass

    def _internal_on_flush(self, client_id: str) -> None:
        self.flush_state(client_id)
        logging.info("Broadcasting EOF downstream to %d routes for client %s", len(self.routes), client_id)
        for route in self.routes:
            eof_msg = serialize(build_eof_message(client=client_id, msg_id=str(uuid.uuid4())))
            route._exchange.send(eof_msg, routing_key="eof_broadcast")

    def _send_to_route(self, route: DateRoute, client_id: str, message: dict, shard_key: Optional[str]) -> None:
        if not message:
            return
        out_msg = serialize(message)

        if route.routing_strategy == "round_robin":
            routing_key = f"worker_{route._next_worker}"
            route._next_worker = (route._next_worker % route.num_downstream_workers) + 1
        elif route.routing_strategy == "sharded":
            if shard_key is None:
                raise ValueError(f"route {route.name!r} uses sharded routing but no shard_key was provided")
            hash_val = zlib.crc32(shard_key.encode("utf-8"))
            target = (hash_val % route.num_downstream_workers) + 1
            routing_key = f"worker_{target}"
        else:
            raise ValueError(f"Unknown routing strategy: {route.routing_strategy}")

        route._exchange.send(out_msg, routing_key=routing_key)

    def stop(self) -> None:
        super().stop()
        for route in self.routes:
            if route._exchange is not None:
                try:
                    route._exchange.close()
                except Exception:
                    logging.exception("error closing route exchange %s", route.name)


def log_config(config: DateRouterConfig) -> None:
    logging.info(
        "DateRouter startup with: mom_host=%s | input_exchange=%s | expected_eofs=%d | routes=%s",
        config.mom_host,
        config.input_exchange,
        config.expected_eofs,
        [(r.name, r.output_exchange, r.from_date.isoformat(), r.to_date.isoformat()) for r in config.routes],
    )


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    worker = DateRouterWorker(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        worker.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    worker.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
