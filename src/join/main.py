import logging
import os
import signal
from dataclasses import dataclass

import threading

import yaml

from src.communication.protocols.queue_protocol.internal import (
    build_batch_message,
    deserialize,
    serialize,
    build_eof_message,
)

from src.common import middleware

from .strategies import (
    JoinStrategy,
    CountStrategy,
    NoStrategy,
    BankMaxAmountStrategy,
)

CONFIG_PATH = "./config.yaml"

@dataclass
class JoinConfig:
    mom_host: str
    input_queue: str
    output_queue: str
    log_level: str
    strategy: JoinStrategy

def _parse_strategy_config(strategy_type: str) -> JoinStrategy:

    if strategy_type == "CountStrategy":
        return CountStrategy()
    
    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy()

    return NoStrategy()

def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}

def init_config() -> JoinConfig:
    file_config = _load_file_config()
    raw_strategy = os.getenv("STRATEGY", file_config.get("strategy", "NoStrategy"))

    return JoinConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy),
    )

def log_config(config: JoinConfig) -> None:
    logging.info(
        "Join startup with: mom_host=%s | input_queue=%s | output_queue=%s | strategy=%s", 
        config.mom_host,
        config.input_queue,
        config.output_queue,
        config.strategy
    )

class JoinService: 
    def __init__(self, config: JoinConfig) -> None:
        self.mom_host = config.mom_host
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.input_queue)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.output_queue)
        self.strategy = config.strategy
        self.lock = threading.Lock()
        self._running = False

    def start(self) -> None:
        self._running = True
        self.input_queue.start_consuming(self.process_data_messsage)

    def stop(self) -> None:
        logging.info("Stopping Join service")
        self._running = False

    def process_data_messsage(self, message, ack, nack):
        message = deserialize(message)
        logging.info("Received message from client %s: %s", message["client"], message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                batch = self.strategy.get_joined_for_client(message["client"])
                
                logging.info("Joined batch: %s", batch)

                batch_message = build_batch_message(
                    message_type="joined_data",
                    client=message["client"],
                    msg_id=message["msg_id"],
                    batch=batch,
                )
                self.output_queue.send(serialize(batch_message))

                eof_message = build_eof_message(
                    client=message["client"],
                    msg_id=message["msg_id"],
                )
                self.output_queue.send(serialize(eof_message))
            else:
                logging.info("Processing data message from client %s", message["client"])               
                self.strategy.join_batch(message["payload"]["batch"], message["client"])

        ack()

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

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
