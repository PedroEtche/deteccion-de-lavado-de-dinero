import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict
import threading

from src.communication.protocols.queue_protocol.internal import (
    build_batch_message,
    deserialize,
    serialize,
    build_eof_message,
)

from src.common import middleware

from .strategies import (
    JoinerStrategy,
    NoStrategy,
    AccountsStrategy,
    SelfMergeStrategy,
)
import yaml


CONFIG_PATH = "./config.yaml"


@dataclass
class JoinerConfig:
    mom_host: str
    input_exchange: str
    shard_id: str
    base_routing_key: str
    output_queue: str
    log_level: str
    strategy: JoinerStrategy


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def _parse_strategy_config(raw_strategy: Dict[str, Any]) -> JoinerStrategy:
    strategy_type = raw_strategy.get("type", "noop")
    params = raw_strategy.get("params", {})

    if strategy_type == "accounts":
        return AccountsStrategy()
    
    if strategy_type == "self_merge":
        return SelfMergeStrategy()

    # if strategy_type == "date":
        # return DateStrategy(
        #     from_date=date.fromisoformat(str(params["from"])),
        #     to_date=date.fromisoformat(str(params["to"]))
        # )

    return NoStrategy()


def init_config() -> JoinerConfig:
    file_config = _load_file_config()
    raw_strategy = file_config.get("strategy", {})

    return JoinerConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_exchange=os.getenv("INPUT_EXCHANGE", file_config.get("input_exchange", "")),
        shard_id=os.getenv("SHARD_ID", file_config.get("shard_id", "")),
        base_routing_key=os.getenv("BASE_ROUTING_KEY", file_config.get("base_routing_key", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy),
    )


def log_config(config: JoinerConfig) -> None:
    logging.info(
        "Joiner startup with: mom_host=%s | input_exchange=%s | shard_id=%s | output_queue=%s | strategy=%s",
        config.mom_host,
        config.input_exchange,
        config.shard_id,
        config.output_queue,
        config.strategy,
    )


class JoinerService:
    def __init__(self, config: JoinerConfig) -> None:
        self.mom_host = config.mom_host

        route = f"{config.base_routing_key}_{config.shard_id}"
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(self.mom_host, config.input_exchange, routing_keys=[route])
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(self.mom_host, config.output_queue)
        
        self.strategy = config.strategy
        self._running = False
        self.lock = threading.Lock()

    def start(self) -> None:
        logging.info("Starting filter service")
        self._running = True
        self.input_exchange.start_consuming(self.process_data_messsage)

    def stop(self) -> None:
        logging.info("Stopping Joiner service")
        self._running = False
        if getattr(self, '_input_middleware', None):
            self._input_middleware.stop_consuming()

    def process_data_messsage(self, message, ack, nack):
        message = deserialize(message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                self.strategy.clear_client_state(message["client"])
                
                eof_message = build_eof_message(client=message["client"], msg_id=message["msg_id"])
                self.output_queue.send(serialize(eof_message))

            else:
                batch = self.strategy.joiner_batch(message["payload"]["batch"], message["client"])

                batch_message = build_batch_message(
                    message_type="grouped_data",
                    client=message["client"],
                    msg_id=message["msg_id"],
                    batch=batch,
                )
                logging.info("Joined batch for client %s", message["client"])
                self.output_queue.send(serialize(batch_message))

        ack()



def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)
    
    service = JoinerService(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    service.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
