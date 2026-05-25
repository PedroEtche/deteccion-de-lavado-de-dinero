import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict

import threading

from strategies import (
    GroupStrategy, 
    NoStrategy, 
    BankMaxAmountStrategy, 
    PaymentFormatAverageStrategy, 
    AccountPairCountStategy,
)
from communication import internal
from common import middleware

import yaml

CONFIG_PATH = "./config.yaml"

@dataclass
class GroupConfig:
    mom_host: str
    input_queue: str
    output_queue: str
    log_level: str
    strategy: GroupStrategy


def _load_file_config() -> Dict[str, str]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}

def _parse_strategy_config(raw_strategy: Dict[str, Any]) -> GroupStrategy:
    strategy_type = raw_strategy.get("type", "noop")
    params = raw_strategy.get("params", {})

    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy()

    if strategy_type == "PaymentFormatAverage":
        return PaymentFormatAverageStrategy()
    
    if strategy_type == "AccountPairCount":
        return AccountPairCountStategy()

    return NoStrategy()

def init_config() -> GroupConfig:
    file_config = _load_file_config()

    return GroupConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(file_config.get("strategy", {})),
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
        self.strategy = config.strategy
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
        message = internal.deserialize(message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                eof_message = internal.build_eof_message(client=message["client"], msg_id=message["msg_id"])
                self.control_exchange.send(internal.serialize(eof_message))

            else: # aca ver condicion para procesar otros mensajes
                logging.info("Processing data message from client %s", message["client"])               
                grouped_batch = self.strategy.join_batch(message["payload"]["batch"])
                logging.info("Grouped batch: %s", grouped_batch)
                
                batch_message = internal.build_batch_message(
                    message_type="grouped_data",
                    client=message["client"],
                    msg_id=message["msg_id"],
                    batch=grouped_batch,
                )
                self.output_queue.send(internal.serialize(batch_message))

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
