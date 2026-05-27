import logging
import os
import signal
from dataclasses import dataclass
import threading
from typing import Any, Dict

import yaml

from src.communication.protocols.queue_protocol.internal import (
    build_batch_message,
    deserialize,
    serialize,
    build_eof_message,
)

from .strategies import (
    AggregatorStrategy,
    NoStrategy,
    BankMaxAmountStrategy,
    AccountPairCountStategy,
)
from src.common import middleware

CONFIG_PATH = "./config.yaml"

@dataclass
class AggregatorConfig:
    mom_host: str
    input_queue: str
    output_queue: str
    log_level: str
    strategy: AggregatorStrategy

def _parse_strategy_config(strategy_type: str) -> AggregatorStrategy:
    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy()

    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy()
    
    if strategy_type == "AccountPairCount":
        return AccountPairCountStategy()

    if strategy_type == "AccountPairCount":
        return AccountPairCountStategy()

    return NoStrategy()

def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}

def init_config() -> AggregatorConfig:
    file_config = _load_file_config()
    raw_strategy = os.getenv("STRATEGY", file_config.get("strategy", "NoStrategy"))

    return AggregatorConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy),
    )

def log_config(config: AggregatorConfig) -> None:
    logging.info(
        "Aggregator startup with: mom_host=%s | input_queue=%s | output_queue=%s | strategy=%s",
        config.mom_host,
        config.input_queue,
        config.output_queue,
        config.strategy
    )

class AggregatorService:
    def __init__(self, config: AggregatorConfig) -> None:
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
        logging.info("Stopping aggregator service")
        self._running = False

    def process_data_messsage(self, message, ack, nack):
        message = deserialize(message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                output_for_client = self.strategy.get_result_for_client(message["client"])
                self.output_queue.send(serialize(
                    build_batch_message(
                        message_type="batch",
                        client=message["client"],
                        msg_id=message["msg_id"],
                        batch=output_for_client,
                    )
                ))
                eof_message = build_eof_message(
                    client=message["client"],
                    msg_id=message["msg_id"],
                )
                self.output_queue.send(serialize(eof_message))
            else:
                logging.info("Processing data message from client %s", message["client"])               
                self.strategy.aggregate_batch(
                    message["payload"]["batch"],
                    message["client"],
                )
        ack()

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    service = AggregatorService(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    service.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
