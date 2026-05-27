import logging
import os
import signal
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict

from .strategies import (
    JoinerStrategy,
    NoStrategy,
    AccountsStrategy,
)
import yaml


CONFIG_PATH = "./config.yaml"


@dataclass
class JoinerConfig:
    mom_host: str
    input_queue: str
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
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        strategy=_parse_strategy_config(raw_strategy),
    )


def log_config(config: JoinerConfig) -> None:
    logging.info(
        "Joiner startup with: mom_host=%s | input_queue=%s | output_queue=%s | strategy=%s",
        config.mom_host,
        config.input_queue,
        config.output_queue,
        str(config.strategy),
    )


class JoinerService:
    def __init__(self, config: JoinerConfig) -> None:
        self.mom_host = config.mom_host
        self.input_queue = config.input_queue
        self.output_queue = config.output_queue
        self.strategy = config.strategy
        self._running = False

    def start(self) -> None:
        logging.info("Starting filter service")
        # eof_control_thread = threading.Thread(target=self._listen_for_eof)
        # eof_control_thread.start()

        self._running = True
        self.input_queue.start_consuming(self.process_data_messsage)

        # eof_control_thread.join()



    def stop(self) -> None:
        logging.info("Stopping Joiner service")
        self._running = False

    def process_data_messsage(self, message, ack, nack):
        message = communication_protocol.deserialize(message)
        with self.lock:
            if message["type"] == "eof":
                logging.info("Received EOF message from client %s", message["client"])
                eof_message = communication_protocol.build_eof_message(client=message["client"], msg_id=message["msg_id"])
                self.control_exchange.send(communication_protocol.serialize(eof_message))

            else:
                logging.info("Processing data message from client %s", message["client"])
                self.strategy.joiner_batch(message["payload"]["batch"])

            # TODO: Esto es el codigo del Group BY, habria que hacer algo similar cuando llega el eof sobre la cola de control de EOF
                # batch_message = communication_protocol.build_batch_message(
                #     message_type="grouped_data",
                #     client=message["client"],
                #     msg_id=message["msg_id"],
                #     batch=grouped_batch,
                # )
                # self.output_queue.send(communication_protocol.serialize(batch_message))

        ack()



def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
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
