import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict, List

import yaml

from src.common.worker import BaseWorker

from .strategies import (
    AmountComparisonStrategy,
    CountFieldComparisonStrategy,
    CurrencyConversionStrategy,
    CurrencyStrategy,
    FilterStrategy,
    NoStrategy,
)

CONFIG_PATH = "./config.yaml"


@dataclass
class FilterConfig:
    mom_host: str
    input_exchange: str
    output_exchange: str
    output_exchanges: List[str]
    log_level: str
    strategy: FilterStrategy
    expected_eofs: int
    worker_id: int
    num_downstream_workers: int
    routing_strategy: str
    worker_name: str
    stage_name: str


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
        "none": lambda _: NoStrategy(),
        "currency": lambda p: CurrencyStrategy(p["value"]),
        "amount": lambda p: AmountComparisonStrategy(p["condition"], p["threshold"]),
        "currency_conversion": lambda p: CurrencyConversionStrategy(p["conditions"]),
        "count": lambda p: CountFieldComparisonStrategy(p["condition"], p["threshold"]),
    }

    builder = _BUILDERS.get(strategy_type)
    if builder is None:
        raise ValueError(f"Unknown strategy type: {strategy_type!r}")
    return builder(params)


def init_config() -> FilterConfig:
    data = _load_file_config()
    # `outputs:` (lista) habilita fan-out a varias ramas; `output:` (single)
    # se sigue soportando y equivale a una lista de 1.
    outputs = data.get("outputs") or (
        [data.get("output")] if data.get("output") else []
    )
    return FilterConfig(
        mom_host=data.get("mom_host", "rabbitmq"),
        input_exchange=data.get("input", ""),
        output_exchange=outputs[0] if outputs else "",
        output_exchanges=outputs,
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        strategy=_build_strategy(data.get("strategy", [])),
        expected_eofs=int(os.getenv("EOF_EXPECTED", "1")),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        num_downstream_workers=int(os.getenv("NUM_DOWNSTREAM_WORKERS", "1")),
        routing_strategy=os.getenv("ROUTING_STRATEGY", "round_robin").lower(),
        worker_name=os.getenv("WORKER_NAME", "filter"),
        stage_name=os.getenv("STAGE_NAME", "filter"),
    )


def log_config(config: FilterConfig) -> None:
    logging.info(
        "Filter startup with: mom_host=%s | input_exchange=%s | output_exchange=%s | expected_eofs=%d | strategy=%s",
        config.mom_host,
        config.input_exchange,
        config.output_exchange,
        config.expected_eofs,
        str(config.strategy),
    )


class FilterWorker(BaseWorker):
    def __init__(self, config: FilterConfig):
        super().__init__(config)
        self.strategy = config.strategy

    def process_batch(self, client_id: str, batch: list, msg_type: str) -> None:
        filtered_batch = self.strategy.filter_batch(batch)
        logging.info("Filtered batch: %d in -> %d out", len(batch), len(filtered_batch))
        self.send(client_id, filtered_batch, "raw_transactions")

    def flush_state(self, client_id: str) -> None:
        pass


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    worker = FilterWorker(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        worker.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    worker.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
