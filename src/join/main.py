import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict

import threading

from strategies import (
    JoinStrategy,
    CountStrategy,
    NoStrategy,
    BankMaxAmountStrategy, 
)
from common import message_protocol, middleware

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

def init_config() -> JoinConfig:
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
        self.lock = threading.Lock()
        self._running = False

    def start(self) -> None:
        logging.info("Starting Join service with strategy %s", self.strategy)
        self._running = True
        self.input_queue.start_consuming(self.process_data_messsage)

    def stop(self) -> None:
        logging.info("Stopping Join service")
        self._running = False

    def process_data_messsage(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        logging.info("Received message from client %s: %s", message["client"], message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                batch = self.strategy.get_joined_for_client(message["client"])
                
                logging.info("Joined batch: %s", batch)

                batch_message = message_protocol.internal.build_batch_message(
                    message_type="joined_data",
                    client=message["client"],
                    msg_id=message["msg_id"],
                    batch=batch,
                )
                self.output_queue.send(message_protocol.internal.serialize(batch_message))

                eof_message = message_protocol.internal.build_eof_message(
                    client=message["client"],
                    msg_id=message["msg_id"],
                )
                self.output_queue.send(message_protocol.internal.serialize(eof_message))

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
