import logging
import os
import signal
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import threading

from .strategies import GroupStrategy, NoStrategy

import yaml

CONFIG_PATH = "./config.yaml"
QUEUE_PROTOCOL_DIR = (
    Path(__file__).resolve().parents[1] / "communication" / "protocols" / "queue-protocol"
)

if str(QUEUE_PROTOCOL_DIR) not in sys.path:
    sys.path.append(str(QUEUE_PROTOCOL_DIR))

import internal

# @dataclass
# class AccountRow(Payload):
#     bank_name: str | None = None
#     bank_id: str | None = None
#     account_number: str | None = None
#     entity_id: str | None = None
#     entity_name: str | None = None


# @dataclass
# class TransactionRow(Payload):
#     timestamp: str | None = None
#     from_bank: str | None = None
#     from_account: str | None = None
#     to_bank: str | None = None
#     to_account: str | None = None
#     amount_received: float | None = None
#     receiving_currency: str | None = None
#     amount_paid: float | None = None
#     payment_currency: str | None = None
#     payment_format: str | None = None

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
    def __init__(self, config: GroupConfig, strategy: GroupStrategy | None = None) -> None:
        self.mom_host = config.mom_host
        self.input_queue = config.input_queue
        self.output_queue = config.output_queue
        self.strategy = strategy or NoStrategy()
        self._running = False

    def start(self) -> None:
        logging.info("Starting group service with strategy %s", self.strategy)

        # eof_control_thread = threading.Thread(target=self._listen_for_eof)
        # eof_control_thread.start()

        self._running = True
        self.input_queue.start_consuming(self.process_data_messsage)

        # eof_control_thread.join()

    def stop(self) -> None:
        logging.info("Stopping group service")
        self._running = False

    def _listen_for_eof(self):
        logging.info("Starting EOF control thread")
        # control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
        #     MOM_HOST, SUM_CONTROL_EXCHANGE, [EOF_BROADCAST]
        # )

        # control_exchange.start_consuming(self._process_eof_message)

    def process_data_messsage(self, message, ack, nack):
        fields = internal.deserialize(message)
        with self.lock:
            if len(fields) == 3:
                client_id, fruit, amount = fields
                
                self._process_data(client_id, fruit, amount)

            elif len(fields) == 1:
                client_id = fields[0]
                self.control_exchange.send(internal.serialize([client_id]))
        ack()
        

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
