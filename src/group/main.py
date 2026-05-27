import logging
import os
import signal
from dataclasses import dataclass
import threading
import yaml
from typing import Any, Dict

from src.communication.protocols.queue_protocol.internal import (
    build_batch_message,
    deserialize,
    serialize,
    build_eof_message,
)

from .strategies import (
    GroupStrategy, 
    NoStrategy, 
    BankMaxAmountStrategy, 
    PaymentFormatAverageStrategy, 
    AccountPairCountStategy,
)

from src.common import middleware

CONFIG_PATH = "./config.yaml"

@dataclass
class GroupConfig:
    mom_host: str
    input_queue: str
    output_exchange: str
    log_level: str
    strategy: GroupStrategy

def _parse_strategy_config(strategy_type: str, base_routing_key: str, total_aggregators: int) -> GroupStrategy:
   
    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy(base_routing_key)

    if strategy_type == "PaymentFormatAverage":
        return PaymentFormatAverageStrategy(base_routing_key, total_aggregators)
    
    if strategy_type == "AccountPairCount":
        return AccountPairCountStategy(base_routing_key)

    return NoStrategy(base_routing_key)

def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}

def init_config() -> GroupConfig:
    file_config = _load_file_config()
    raw_strategy = os.getenv("STRATEGY", file_config.get("strategy", "NoStrategy"))

    total_aggregators = int(os.getenv("TOTAL_AGGREGATORS", file_config.get("total_aggregators", "1")))
    base_routing_key = os.getenv("BASE_ROUTING_KEY", file_config.get("base_routing_key", ""))

    return GroupConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_exchange=os.getenv("OUTPUT_EXCHANGE", file_config.get("output_exchange", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy, base_routing_key, total_aggregators),
    )

def log_config(config: GroupConfig) -> None:
    logging.info(
        "Group startup with: mom_host=%s | input_queue=%s | output_exchange=%s | strategy=%s",
        config.mom_host,
        config.input_queue,
        config.output_exchange,
        config.strategy
    )

class GroupService:
    def __init__(self, config: GroupConfig) -> None:
        self.mom_host = config.mom_host
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.input_queue)
        
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            host=self.mom_host, 
            exchange_name=config.output_exchange
        )

        self.strategy = config.strategy
        self.lock = threading.Lock()
        self._running = False

    def start(self) -> None:
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
        message = deserialize(message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                eof_message = build_eof_message(
                    client=message["client"],
                    msg_id=message["msg_id"],
                )

                eof_routes = self.strategy.get_eof_routes()
                logging.info("Sending EOF message to routes: %s", eof_routes)
                for route in eof_routes:
                    logging.info("Sending EOF message to route: %s", route)
                    self._output_exchange.send(serialize(eof_message), routing_key=route)

            else: # aca ver condicion para procesar otros mensajes
                logging.info("Processing data message from client %s", message["client"])
                
                # Fetch dynamically routed grouped batches from the strategy
                routed_batches = self.strategy.group_and_route(message["payload"]["batch"])
                
                for route, grouped_batch in routed_batches:
                    batch_message = build_batch_message(
                        message_type="batch",
                        client=message["client"],
                        msg_id=message["msg_id"],
                        batch=grouped_batch,
                    )
                    logging.info("Sending grouped batch message to output queue %s: %s", route, batch_message)
                    self._output_exchange.send(serialize(batch_message), routing_key=route)

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
