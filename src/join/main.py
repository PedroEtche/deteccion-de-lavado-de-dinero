import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict

import threading

from strategies import (
    JoinStrategy, 
    NoStrategy, 
)
from common import message_protocol, middleware

import yaml

CONFIG_PATH = "./config.yaml"

@dataclass
class JoinConfig:
    mom_host: str
    input_queue: str
    output_queue: str
    log_level: str
    strategy: JoinStrategy


def _load_file_config() -> Dict[str, str]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        logging.warning("Configuration file not found at %s. Using defaults and environment variables.", CONFIG_PATH)
        return {}

def _parse_strategy_config(strategy_type: str) -> JoinStrategy:
    # strategy_type = raw_strategy.get("type", "noop")
    # params = raw_strategy.get("params", {})

    if strategy_type == "BankMaxAmountStrategy":
        return NoStrategy()

    return NoStrategy()

def init_config() -> JoinConfig:
    # file_config = _load_file_config()
    # logging.info("Loaded file config: %s", file_config)
    return JoinConfig(
        mom_host=os.environ["MOM_HOST"],
        input_queue=os.environ["INPUT_QUEUE"],
        output_queue=os.environ["OUTPUT_QUEUE"],
        log_level=os.environ["LOG_LEVEL"],
        strategy=_parse_strategy_config(os.environ.get("STRATEGY", "NoStrategy")),
    )

def log_config(config: JoinConfig) -> None:
    logging.info(
        "Join startup with: mom_host=%s | input_queue=%s | output_queue=%s",
        config.mom_host,
        config.input_queue,
        config.output_queue,
    )

class JoinService: 
    def __init__(self, config: JoinConfig) -> None:
        logging.info("Initializing JoinService with strategy: %s", config.strategy)
        self.mom_host = config.mom_host
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.input_queue)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.output_queue)
        self.strategy = config.strategy
        self._running = False

    def start(self) -> None:
        logging.info("Starting Join service with strategy %s", self.strategy)

        # eof_control_thread = threading.Thread(target=self._listen_for_eof)
        # eof_control_thread.start()

        self._running = True
        self.input_queue.start_consuming(self.process_data_messsage)

        # eof_control_thread.join()

    def stop(self) -> None:
        logging.info("Stopping Join service")
        self._running = False

    def _listen_for_eof(self):
        logging.info("Starting EOF control thread")
        # control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
        #     MOM_HOST, SUM_CONTROL_EXCHANGE, [EOF_BROADCAST]
        # )

        # control_exchange.start_consuming(self._process_eof_message)

    def process_data_messsage(self, message, ack, nack):
        message = message_protocol.deserialize(message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                eof_message = message_protocol.build_eof_message(client=message["client"], msg_id=message["msg_id"])
                self.control_exchange.send(message_protocol.serialize(eof_message))

            else: # aca ver condicion para procesar otros mensajes
                logging.info("Processing data message from client %s", message["client"])               
                grouped_batch = self.strategy.group_batch(message["payload"]["batch"])
                logging.info("Grouped batch: %s", grouped_batch)
                
                batch_message = message_protocol.build_batch_message(
                    message_type="grouped_data",
                    client=message["client"],
                    msg_id=message["msg_id"],
                    batch=grouped_batch,
                )
                self.output_queue.send(message_protocol.serialize(batch_message))

        ack()

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.debug("Initialized configuration: %s", config)
    service = JoinService(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    service.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
