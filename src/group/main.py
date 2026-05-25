import logging
import os
import signal
from dataclasses import dataclass
import threading

from strategies import (
    GroupStrategy, 
    NoStrategy, 
    BankMaxAmountStrategy, 
    PaymentFormatAverageStrategy, 
    AccountPairCountStategy,
)
from common import message_protocol, middleware

@dataclass
class GroupConfig:
    mom_host: str
    input_queue: str
    output_queue: str
    log_level: str
    strategy: GroupStrategy

def _parse_strategy_config(strategy_type: str) -> GroupStrategy:
   
    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy()

    if strategy_type == "PaymentFormatAverage":
        return PaymentFormatAverageStrategy()
    
    if strategy_type == "AccountPairCount":
        return AccountPairCountStategy()

    return NoStrategy()

def init_config() -> GroupConfig:
    return GroupConfig(
        mom_host=os.environ["MOM_HOST"],
        input_queue=os.environ["INPUT_QUEUE"],
        output_queue=os.environ["OUTPUT_QUEUE"],
        log_level=os.environ["LOG_LEVEL"],
        strategy=_parse_strategy_config(os.environ["STRATEGY"]),
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
        logging.debug("Initializing GroupService with strategy: %s", config.strategy)
        self.mom_host = config.mom_host
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.input_queue)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.output_queue)
        self.strategy = config.strategy
        self.lock = threading.Lock()
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

    def process_mock_data(self, message):
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                eof_message = message_protocol.internal.build_eof_message(
                    client=message["client"],
                    msg_id=message["msg_id"],
                )
                self.control_exchange.send(message_protocol.internal.serialize(eof_message))

            else: # aca ver condicion para procesar otros mensajes
                logging.info("Processing data message from client %s", message["client"])               
                grouped_batch = self.strategy.group_batch(message["payload"]["batch"])
                logging.info("Grouped batch: %s", grouped_batch)
                
                batch_message = message_protocol.internal.build_batch_message(
                    message_type="batch",
                    client=message["client"],
                    msg_id=message["msg_id"],
                    batch=grouped_batch,
                )
                logging.info("Sending grouped batch message to output queue: %s", batch_message)
                self.output_queue.send(message_protocol.internal.serialize(batch_message))

                eof_message = message_protocol.internal.build_eof_message(
                    client=message["client"],
                    msg_id=message["msg_id"],
                )
                self.output_queue.send(message_protocol.internal.serialize(eof_message))

    def process_data_messsage(self, message, ack, nack):
        message = message_protocol.internal.deserialize(message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                eof_message = message_protocol.internal.build_eof_message(
                    client=message["client"],
                    msg_id=message["msg_id"],
                )
                self.control_exchange.send(message_protocol.internal.serialize(eof_message))

            else: # aca ver condicion para procesar otros mensajes
                logging.info("Processing data message from client %s", message["client"])               
                grouped_batch = self.strategy.group_batch(message["payload"]["batch"])
                logging.info("Grouped batch: %s", grouped_batch)
                
                batch_message = message_protocol.internal.build_batch_message(
                    message_type="batch",
                    client=message["client"],
                    msg_id=message["msg_id"],
                    batch=grouped_batch,
                )
                logging.info("Sending grouped batch message to output queue: %s", batch_message)
                self.output_queue.send(message_protocol.internal.serialize(batch_message))

        ack()

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

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
