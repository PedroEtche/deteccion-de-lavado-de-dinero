import logging
import os
import signal
import uuid
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional

from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
from src.common.communication.internal import (
    build_eof_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)
from src.common.eof import EofCoordinator


@dataclass
class DateRoute:
    name: str
    output_exchange: str
    from_date: date
    to_date: date
    num_downstream_workers: int = 1
    _exchange: Optional[MessageMiddlewareExchangeRabbitMQ] = field(
        default=None, init=False, repr=False
    )
    _next_worker: int = field(default=1, init=False, repr=False)

    def matches(self, day: date) -> bool:
        return self.from_date <= day <= self.to_date


# Rangos fijos del enunciado:
#   [2022-09-01, 2022-09-05] → Q3 (promedio por payment_format), Q4, Q5
#   [2022-09-06, 2022-09-15] → Q3 (filtro de monto pequeño)
def _default_routes(num_downstream_workers: int) -> List[DateRoute]:
    return [
        DateRoute(
            name="dates_1_to_5",
            output_exchange="trx_dates_1_to_5",
            from_date=date(2022, 9, 1),
            to_date=date(2022, 9, 5),
            num_downstream_workers=num_downstream_workers,
        ),
        DateRoute(
            name="dates_6_to_15",
            output_exchange="trx_dates_6_to_15",
            from_date=date(2022, 9, 6),
            to_date=date(2022, 9, 15),
            num_downstream_workers=num_downstream_workers,
        ),
    ]


@dataclass
class DateRouterConfig:
    mom_host: str
    input_exchange: str
    log_level: str
    expected_eofs: int
    worker_id: int
    routes: List[DateRoute]


def init_config() -> DateRouterConfig:
    num_downstream_workers = int(os.getenv("NUM_DOWNSTREAM_WORKERS", "1"))
    return DateRouterConfig(
        mom_host=os.getenv("MOM_HOST", "rabbitmq"),
        input_exchange=os.getenv("INPUT_EXCHANGE", ""),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        expected_eofs=int(os.getenv("EOF_EXPECTED", "1")),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        routes=_default_routes(num_downstream_workers),
    )


class DateRouterWorker:
    """Stateless router. Each row is forwarded to every route whose date range matches.

    Rows with an unparseable timestamp or no matching route are dropped.
    A row matching N routes produces N independent sub-batches downstream.

    Standalone (does not inherit from BaseWorker): the single-output assumptions
    in BaseWorker do not fit this multi-output worker.
    """

    def __init__(self, config: DateRouterConfig) -> None:
        self.config = config
        self.routes: List[DateRoute] = list(config.routes)
        self.input_exchange: Optional[MessageMiddlewareExchangeRabbitMQ] = None
        self.eof_coordinator: Optional[EofCoordinator] = None

    def start(self) -> None:
        logging.info("Starting %s...", self.__class__.__name__)

        self.eof_coordinator = EofCoordinator(
            expected_eofs=self.config.expected_eofs,
            on_flush=self._on_flush,
        )

        routing_keys = [f"worker_{self.config.worker_id}", "eof_broadcast"]
        self.input_exchange = MessageMiddlewareExchangeRabbitMQ(
            self.config.mom_host, self.config.input_exchange, routing_keys
        )

        for route in self.routes:
            route._exchange = MessageMiddlewareExchangeRabbitMQ(
                self.config.mom_host, route.output_exchange
            )

        self.input_exchange.start_consuming(self._on_message)

    def _on_message(self, message: bytes, ack, nack) -> None:
        try:
            decoded = deserialize(message)
            msg_type = decoded.get("type")
            client_id = decoded.get("client")

            if msg_type == "eof":
                logging.info("Received EOF from upstream for client %s", client_id)
                self.eof_coordinator.handle_eof(client_id)
            else:
                logging.info("Received message of type %s for client %s", msg_type, client_id)
                self.process_data(client_id, decoded["payload"])
            ack()
        except Exception:
            logging.exception("Error processing message")
            nack()

    def process_data(self, client_id: str, payload: dict) -> None:
        batch = payload.get("batch", [])
        if not batch:
            return

        buckets: Dict[str, List[Any]] = {route.name: [] for route in self.routes}
        for row in batch:
            row_dt = getattr(row, "date", None)
            if row_dt is None:
                continue
            row_day = row_dt.date()
            for route in self.routes:
                if route.matches(row_day):
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
            self._send_to_route(route, out_msg)

    def _on_flush(self, client_id: str) -> None:
        logging.info("Broadcasting EOF downstream to %d routes for client %s", len(self.routes), client_id)
        for route in self.routes:
            eof_msg = serialize(build_eof_message(client=client_id, msg_id=str(uuid.uuid4())))
            route._exchange.send(eof_msg, routing_key="eof_broadcast")

    def _send_to_route(self, route: DateRoute, message: dict) -> None:
        if not message:
            return
        out_msg = serialize(message)

        routing_key = f"worker_{route._next_worker}"
        route._next_worker = (route._next_worker % route.num_downstream_workers) + 1

        route._exchange.send(out_msg, routing_key=routing_key)

    def stop(self) -> None:
        if self.input_exchange is not None:
            self.input_exchange.stop_consuming()
            self.input_exchange.close()
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
