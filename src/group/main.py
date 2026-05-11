import logging
import os
import signal
from dataclasses import dataclass
from typing import Dict, List, Tuple

import yaml


CONFIG_PATH = "./config.yaml"


@dataclass
class GroupConfig:
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


def init_config() -> GroupConfig:
    file_config = _load_file_config()
    return GroupConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
    )


def log_config(config: GroupConfig) -> None:
    logging.info(
        "Group startup with: mom_host=%s | input_queue=%s | output_queue=%s",
        config.mom_host,
        config.input_queue,
        config.output_queue,
    )


class GroupService:
    def __init__(self, config: GroupConfig) -> None:
        self.mom_host = config.mom_host
        self.input_queue = config.input_queue
        self.output_queue = config.output_queue
        self._running = False

    def start(self) -> None:
        logging.info("Starting group service")
        self._running = True
        # Placeholder for message loop integration.

    def stop(self) -> None:
        logging.info("Stopping group service")
        self._running = False


def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    service = GroupService(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    service.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
