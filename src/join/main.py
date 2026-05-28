import logging
import os
import signal
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

import yaml

from src.common import middleware
from src.common.eof import EofCoordinator
from src.communication.protocols.queue_protocol.internal import (
    build_batch_message,
    build_eof_message,
    deserialize,
    serialize,
)

from .strategies import (
    UnionStrategy,
    BankMaxAmountStrategy,
    CountStrategy,
    JoinStrategy,
    NoStrategy,
    AccountStrategy,
)

CONFIG_PATH = "./config.yaml"


@dataclass
class JoinConfig:
    mom_host: str
    input_queue: str
    output_queue: str
    log_level: str
    eof_fanout: str
    expected_eofs: int
    strategy: JoinStrategy
    accounts_input_queue: Optional[str] = None


def _extract_strategy_type(raw_strategy) -> str:
    if isinstance(raw_strategy, dict):
        return str(raw_strategy.get("type", "NoStrategy"))
    return str(raw_strategy or "NoStrategy")


def _parse_strategy_config(raw_strategy) -> JoinStrategy:
    strategy_type = _extract_strategy_type(raw_strategy)
    if strategy_type in ("CountStrategy", "Count"):
        return CountStrategy()
    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy()
    if strategy_type in ("JoinUnion", "UnionStrategy"):
        return UnionStrategy()
    if strategy_type == "Account":
        return AccountStrategy()
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
    accounts_input_queue = os.getenv(
        "ACCOUNTS_INPUT_QUEUE",
        file_config.get("accounts_input_queue", ""),
    ) or None

    return JoinConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_queue=os.getenv("INPUT_QUEUE", file_config.get("input_queue", "")),
        output_queue=os.getenv("OUTPUT_QUEUE", file_config.get("output_queue", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        eof_fanout=os.getenv("EOF_FANOUT", file_config.get("eof_fanout", "")),
        expected_eofs=int(os.getenv("EXPECTED_EOFS", file_config.get("expected_eofs", "1"))),
        strategy=_parse_strategy_config(raw_strategy),
        accounts_input_queue=accounts_input_queue,
    )


def log_config(config: JoinConfig) -> None:
    logging.info(
        "Join startup with: mom_host=%s | input_queue=%s | output_queue=%s | "
        "accounts_input_queue=%s | eof_fanout=%s | expected_eofs=%d | strategy=%s",
        config.mom_host,
        config.input_queue,
        config.output_queue,
        config.accounts_input_queue,
        config.eof_fanout,
        config.expected_eofs,
        config.strategy,
    )


class JoinService:
    """Single-input join. Cuenta `expected_eofs` antes de emitir el resultado.

    Si `accounts_input_queue` está seteada (ej. Q2), un thread separado consume
    raw_accounts en paralelo y las inyecta en la strategy bajo el lock del
    coordinator. Se asume FIFO-por-productor: el cliente envía accounts antes
    que las transacciones, así que el accounts EOF llega antes del flush.
    """

    def __init__(self, config: JoinConfig) -> None:
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            config.mom_host, config.input_queue
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            config.mom_host, config.output_queue
        )
        self.strategy = config.strategy
        self.coord = EofCoordinator(
            mom_host=config.mom_host,
            fanout_name=config.eof_fanout,
            expected_eofs=config.expected_eofs,
            on_flush=self._flush_client,
        )
        self._accounts_queue_name = config.accounts_input_queue
        self._accounts_mom_host = config.mom_host
        self._accounts_input_queue = None
        self._accounts_thread: Optional[threading.Thread] = None
        self._flushed_clients = set()

    def start(self) -> None:
        self.coord.start()
        if self._accounts_queue_name:
            self._accounts_thread = threading.Thread(
                target=self._consume_accounts,
                name="accounts-consumer",
                daemon=True,
            )
            self._accounts_thread.start()
        self.input_queue.start_consuming(self._on_input)

    def stop(self) -> None:
        logging.info("Stopping join service")
        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.exception("error stopping input consumer")
        if self._accounts_input_queue is not None:
            try:
                self._accounts_input_queue.stop_consuming()
            except Exception:
                logging.exception("error stopping accounts consumer")
        self.coord.stop(timeout=10)

    def close(self) -> None:
        for closeable in (self.input_queue, self.output_queue, self._accounts_input_queue):
            if closeable is None:
                continue
            try:
                closeable.close()
            except Exception:
                logging.exception("error closing middleware")
        self.coord.close()

    def _consume_accounts(self) -> None:
        try:
            self._accounts_input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                self._accounts_mom_host, self._accounts_queue_name
            )
            self._accounts_input_queue.start_consuming(self._on_accounts)
        except Exception:
            logging.exception("error consuming accounts queue")

    def _on_accounts(self, message, ack, _nack):
        decoded = deserialize(message)
        msg_type = decoded.get("type")
        client = decoded.get("client")
        if msg_type == "eof":
            logging.info("Accounts EOF received for client %s", client)
        elif msg_type == "raw_accounts":
            with self.coord.lock():
                self.strategy.add_accounts(decoded["payload"]["batch"], client)
        else:
            logging.warning("Unknown accounts message type: %s", msg_type)
        ack()

    def _on_input(self, message, ack, _nack):
        decoded = deserialize(message)
        client = decoded["client"]
        if decoded["type"] == "eof":
            logging.info("Received EOF from upstream for client %s", client)
            self.coord.broadcast(client)
        else:
            with self.coord.lock():
                logging.debug("Joining batch for client %s", client)
                self.strategy.join_batch(decoded["payload"]["batch"], client)
        ack()

    def _flush_client(self, client: str) -> None:
        """Bajo `coord.lock()`. Emite el resultado joineado + EOF downstream."""
        logging.info("Flushing joined result for client %s", client)
        
        if client in self._flushed_clients:
            logging.info("Ignoring duplicate flush wave for client %s", client)
            return
        self._flushed_clients.add(client)

        batch = self.strategy.get_joined_for_client(client)
        self.output_queue.send(
            serialize(
                build_batch_message(
                    message_type="joined_data",
                    client=client,
                    msg_id=str(uuid.uuid4()),
                    batch=batch,
                )
            )
        )
        self.output_queue.send(
            serialize(build_eof_message(client=client, msg_id=str(uuid.uuid4())))
        )

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    service = JoinService(config)

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        service.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    try:
        service.start()
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
