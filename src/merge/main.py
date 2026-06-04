import logging
import os
import signal
import uuid
from dataclasses import dataclass
from typing import Any, Dict

import yaml

from src.common import middleware
from src.common.eof import EofCoordinator
from src.common.communication.internal import (
    build_batch_message,
    deserialize,
    serialize,
    build_eof_message,
)

from .strategies import (
    JoinerStrategy,
    NoStrategy,
    AccountsStrategy,
    SelfMergeStrategy,
)

CONFIG_PATH = "./config.yaml"


@dataclass
class JoinerConfig:
    mom_host: str
    input_exchange: str
    shard_id: str
    base_routing_key: str
    output_queue: str
    log_level: str
    eof_fanout: str
    expected_eofs: int
    strategy: JoinerStrategy


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def _parse_strategy_config(
    raw_strategy: Dict[str, Any], shard_id: int, shard_amount: int
) -> JoinerStrategy:
    strategy_type = (
        raw_strategy.get("type", "noop")
        if isinstance(raw_strategy, dict)
        else str(raw_strategy)
    )

    if strategy_type == "accounts":
        return AccountsStrategy()

    if strategy_type == "self_merge":
        return SelfMergeStrategy(shard_amount=shard_amount, shard_id=shard_id)

    return NoStrategy()


def init_config() -> JoinerConfig:
    file_config = _load_file_config()
    raw_strategy = file_config.get("strategy", {})
    raw_params = (
        raw_strategy.get("params", {}) if isinstance(raw_strategy, dict) else {}
    )

    shard_id = int(os.getenv("SHARD_ID", file_config.get("shard_id", "0")) or 0)
    shard_amount = int(
        os.getenv(
            "SHARD_AMOUNT",
            raw_params.get("shard_amount", file_config.get("shard_amount", 1)),
        )
    )

    return JoinerConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_exchange=os.getenv(
            "INPUT_EXCHANGE", file_config.get("input_exchange", "")
        ),
        shard_id=str(shard_id),
        base_routing_key=os.getenv(
            "BASE_ROUTING_KEY", file_config.get("base_routing_key", "")
        ),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        eof_fanout=os.getenv("EOF_FANOUT", file_config.get("eof_fanout", "")),
        expected_eofs=int(
            os.getenv("EXPECTED_EOFS", file_config.get("expected_eofs", "1"))
        ),
        strategy=_parse_strategy_config(raw_strategy, shard_id, shard_amount),
    )


def log_config(config: JoinerConfig) -> None:
    logging.info(
        "Joiner startup with: mom_host=%s | input_exchange=%s | shard_id=%s | output_queue=%s | "
        "eof_fanout=%s | expected_eofs=%d | strategy=%s",
        config.mom_host,
        config.input_exchange,
        config.shard_id,
        config.output_queue,
        config.eof_fanout,
        config.expected_eofs,
        config.strategy,
    )


class JoinerService:
    """Stateful joiner. Cuenta `expected_eofs` antes de flushear por cliente."""

    def __init__(self, config: JoinerConfig) -> None:
        self.mom_host = config.mom_host
        self.strategy = config.strategy

        route = f"{config.base_routing_key}_{config.shard_id}"
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            self.mom_host, config.input_exchange, routing_keys=[route]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            self.mom_host, config.output_queue
        )
        self.coord = EofCoordinator(
            mom_host=config.mom_host,
            fanout_name=config.eof_fanout,
            expected_eofs=config.expected_eofs,
            on_flush=self._flush_client,
        )

    def start(self) -> None:
        logging.info("Starting Joiner service")
        self.coord.start()
        self.input_exchange.start_consuming(self._on_input)

    def stop(self) -> None:
        logging.info("Stopping Joiner service")
        try:
            self.input_exchange.stop_consuming()
        except Exception:
            logging.exception("error stopping input consumer")
        self.coord.stop(timeout=10)

    def close(self) -> None:
        for closeable in (self.input_exchange, self.output_queue):
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
                batch = self.strategy.joiner_batch(decoded["payload"]["batch"], client)

                if batch:
                    batch_msg = build_batch_message(
                        message_type="grouped_data",
                        client=client,
                        msg_id=str(uuid.uuid4()),
                        batch=batch,
                    )
                    self.output_queue.send(serialize(batch_msg))

        ack()

    def _flush_client(self, client: str) -> None:
        """Se invoca bajo `coord.lock()` cuando llegaron `expected_eofs` EOFs."""
        logging.info("Flushing joiner state for client %s", client)

        if hasattr(self.strategy, "clear_client_state"):
            self.strategy.clear_client_state(client)

        eof_msg = build_eof_message(client=client, msg_id=str(uuid.uuid4()))
        self.output_queue.send(serialize(eof_msg))


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    service = JoinerService(config)

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
