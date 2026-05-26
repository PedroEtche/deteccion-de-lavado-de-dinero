import logging
import os
import signal
from dataclasses import dataclass
import threading

from strategies import (
    AggregatorStrategy,
    NoStrategy,
    BankMaxAmountStrategy,
    AccountPairCountStategy,
)
from common import message_protocol, middleware

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

def init_config() -> AggregatorConfig:
    return AggregatorConfig(
        mom_host=os.environ["MOM_HOST"],
        input_queue=os.environ["INPUT_QUEUE"],
        output_queue=os.environ["OUTPUT_QUEUE"],
        log_level=os.environ["LOG_LEVEL"],
        strategy=_parse_strategy_config(os.environ["STRATEGY"]),
    )

def log_config(config: AggregatorConfig) -> None:
    logging.info(
        "Aggregator startup with: mom_host=%s | input_queue=%s | output_queue=%s",
        config.mom_host,
        config.input_queue,
        config.output_queue,
    )

class AggregatorService:
    def __init__(self, config: AggregatorConfig) -> None:
        logging.debug("Initializing AggregatorService with strategy: %s", config.strategy)
        self.mom_host = config.mom_host
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.input_queue)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.output_queue)
        self.strategy = config.strategy
        self.lock = threading.Lock()
        self._running = False

    def start(self) -> None:
        logging.info("Starting group service with strategy %s", self.strategy)
        self._running = True
        self.input_queue.start_consuming(self.process_data_messsage)

    def stop(self) -> None:
        logging.info("Stopping group service")
        self._running = False

    def process_data_messsage(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                output_for_client = self.strategy.get_result_for_client(message["client"])
                self.output_queue.send(message_protocol.internal.serialize(
                    message_protocol.internal.build_batch_message(
                        message_type="batch",
                        client=message["client"],
                        msg_id=message["msg_id"],
                        batch=output_for_client,
                    )
                ))
                eof_message = message_protocol.internal.build_eof_message(
                    client=message["client"],
                    msg_id=message["msg_id"],
                )
                self.output_queue.send(message_protocol.internal.serialize(eof_message))

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
