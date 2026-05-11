import logging
import os
import signal
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict

import yaml

CONFIG_PATH = "./config.yaml"

@dataclass
class CurrencyConverterConfig:
    mom_host: str
    input_queue: str
    output_queue: str
    log_level: str


def _load_file_config() -> Dict[str, str]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def init_config() -> CurrencyConverterConfig:
    file_config = _load_file_config()
    return CurrencyConverterConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
    )


def log_config(config: CurrencyConverterConfig) -> None:
    logging.info(
        "Currency converter startup with: mom_host=%s | input_queue=%s | output_queue=%s",
        config.mom_host,
        config.input_queue,
        config.output_queue,
    )


class CurrencyConverterService:
    def __init__(self, config: CurrencyConverterConfig) -> None:
        self.mom_host = config.mom_host
        self.input_queue = config.input_queue
        self.output_queue = config.output_queue
        self.exchange_rates: Dict[str, Decimal] = {}
        self._running = False

    def start(self) -> None:
        logging.info("Starting currency converter service")
        self._running = True
        # Placeholder for message loop integration.

    def stop(self) -> None:
        logging.info("Stopping currency converter service")
        self._running = False


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    service = CurrencyConverterService(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    service.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
