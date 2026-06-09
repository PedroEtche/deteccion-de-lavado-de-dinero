import logging
import os
import signal
import uuid
from dataclasses import dataclass
from typing import Any, Dict

import yaml

from src.common.worker import BaseWorker
from src.common.communication.internal import build_batch_message

from .strategies import (
    MergeStrategy,
    NoStrategy,
    AccountsStrategy,
    # SelfMergeStrategy,
)

CONFIG_PATH = "./config.yaml"

@dataclass
class MergeConfig:
    mom_host: str
    input_exchange: str
    output_exchange: str
    log_level: str
    expected_eofs: int
    worker_id: int
    num_downstream_workers: int
    routing_strategy: str
    strategy: MergeStrategy

def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}

def _parse_strategy_config(raw_strategy: Any) -> MergeStrategy:
    strategy_type = _read_strategy_type(raw_strategy)

    if strategy_type == "accounts":
        return AccountsStrategy()

    if strategy_type == "self_merge":
        return SelfMergeStrategy()

    return NoStrategy()

def _read_strategy_type(raw_strategy: Any) -> str:
    if isinstance(raw_strategy, dict):
        return str(raw_strategy.get("type", "NoStrategy"))
    return str(raw_strategy or "NoStrategy")

def init_config() -> MergeConfig:
    data = _load_file_config()
    raw_strategy = data.get("strategy", "NoStrategy")

    return MergeConfig(
       mom_host=data.get("mom_host", "rabbitmq"),
        input_exchange=data.get("input", ""),
        output_exchange=data.get("output", ""),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        strategy=_parse_strategy_config(raw_strategy),
        expected_eofs=int(os.getenv("EOF_EXPECTED", "1")),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        num_downstream_workers=int(os.getenv("NUM_DOWNSTREAM_WORKERS", "1")),
        routing_strategy=os.getenv("ROUTING_STRATEGY", "round_robin").lower()
    )

def log_config(config: MergeConfig) -> None:
    logging.info(
        "Merge startup with: mom_host=%s | input_exchange=%s | output_exchange=%s | "
        "worker_id=%d | expected_eofs=%d | strategy=%s",
        config.mom_host,
        config.input_exchange,
        config.output_exchange,
        config.worker_id,
        config.expected_eofs,
        config.strategy,
    )

class MergeWorker(BaseWorker):
    """
    Stateful Merge. Uses a strategy to accumulate data in memory.
    Flushes state only when expected_eofs is reached.
    """
    def __init__(self, config: MergeConfig):
        super().__init__(config)

    def process_data(self, client_id: str, msg_id: str, msg_type: str, payload: dict) -> None:
        batch = payload.get("batch", [])
    
        merged_batch = self.strategy.merge_batch(batch, client_id, msg_type)

        # revisar para poner un callback aca
        if merged_batch:
            batch_msg = build_batch_message(
                message_type="grouped_data",
                client=client_id,
                msg_id=str(uuid.uuid4()),
                batch=merged_batch,
            )
            self.send_downstream(client_id, batch_msg)

    def flush_state(self, client_id: str) -> None:
        logging.info("All EOFs received. Flushing joiner state for client %s", client_id)
        
        self.strategy.clear_client_state(client_id)

        # aca ver que no este faltando flushear algo    

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    worker = MergeWorker(config)

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        worker.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    # Start blocks and consumes
    worker.start()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
