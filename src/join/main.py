import logging
import os
import signal
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List

import yaml

from src.common.communication.internal import (
    serialize,
)
from src.common.worker import BaseWorker

from .strategies import (
    JoinStrategy,
    NoStrategy,
    Q1Strategy,
)

CONFIG_PATH = "./config.yaml"


@dataclass
class JoinConfig:
    mom_host: str
    input_exchange: str
    output_exchange: str
    log_level: str
    strategy: JoinStrategy
    expected_eofs: int
    worker_id: int
    num_downstream_workers: int
    routing_strategy: str


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def _build_strategy(strategy_data: List[Dict[str, Any]]) -> JoinStrategy:
    params: Dict[str, Any] = {}
    for item in strategy_data:
        params.update(item)

    strategy_type = params.get("type", "none")

    _BUILDERS = {
        "none": lambda _: NoStrategy(),
        "q1": lambda _: Q1Strategy(),
    }

    builder = _BUILDERS.get(strategy_type)
    if builder is None:
        raise ValueError(f"Unknown strategy type: {strategy_type!r}")
    return builder(params)


def init_config() -> JoinConfig:
    data = _load_file_config()
    return JoinConfig(
        mom_host=data.get("mom_host", "rabbitmq"),
        input_exchange=data.get("input", ""),
        output_exchange=data.get("output", ""),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        strategy=_build_strategy(data.get("strategy", [])),
        expected_eofs=int(os.getenv("EOF_EXPECTED", "1")),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        num_downstream_workers=int(os.getenv("NUM_DOWNSTREAM_WORKERS", "1")),
        routing_strategy=os.getenv("ROUTING_STRATEGY", "round_robin").lower(),
    )


def log_config(config: JoinConfig) -> None:
    logging.info(
        "Join startup with: mom_host=%s | input_exchange=%s | output_exchange=%s | strategy=%s | expected_eofs=%d",
        config.mom_host,
        config.input_exchange,
        config.output_exchange,
        str(config.strategy),
        config.expected_eofs,
    )


class JoinWorker(BaseWorker):
    def __init__(self, config: JoinConfig):
        super().__init__(config)
        self.strategy = config.strategy
        # The strategy emits ready batches mid-stream through this callback.
        self.strategy.register_join_callback(self.send_downstream)

    def process_data(self, client_id: str, msg_id: str, payload: dict) -> None:
        batch = payload.get("batch", [])
        logging.info("Join batch: %d rows in for client %s", len(batch), client_id)
        self.strategy.join_batch(batch, client_id)

    def flush_state(self, client_id: str) -> None:
        final_msg = self.strategy.flush(client_id)
        if final_msg:
            self.send_downstream(client_id, final_msg)


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    worker = JoinWorker(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        worker.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    worker.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
