import logging
import os
import signal
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict
from .strategies import (
    FilterStrategy,
    NoStrategy,
    CurrencyStrategy,
    DateStrategy,
)
import yaml


CONFIG_PATH = "./config.yaml"


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


def _parse_strategy_config(raw_strategy: Dict[str, Any]) -> FilterStrategy:
    strategy_type = raw_strategy.get("type", "noop")
    params = raw_strategy.get("params", {})

    if strategy_type == "currency":
        return CurrencyStrategy(params["target_currency"])

    if strategy_type == "date":
        return DateStrategy(
            from_date=date.fromisoformat(str(params["from"])),
            to_date=date.fromisoformat(str(params["to"]))
        )

    return NoStrategy()


def init_config() -> FilterConfig:
    file_config = _load_file_config()
    raw_strategy = file_config.get("strategy", {})

    return FilterConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy),
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
        self.mom_host = config.mom_host
        self.input_queue = config.input_queue
        self.output_queue = config.output_queue
        self.strategy = config.strategy
        self._running = False

    def start(self) -> None:
        logging.info("Starting filter service")
        self._running = True
        # Placeholder for message loop integration.

        # INFO: Message loop pseudocode example with strategy application:
        # Receive message
        # result = self.strategy.filter(message)
        # send result

    def stop(self) -> None:
        logging.info("Stopping filter service")
        self._running = False


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
