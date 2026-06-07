import logging
import os
import signal
import uuid
from dataclasses import dataclass
from typing import Any, Dict

import yaml

from src.common.worker import BaseWorker
from src.common.communication.internal import (
    build_batch_message,
)

from .strategies import (
    AccountPairCountStategy,
    AccountStrategy,
    BankMaxAmountStrategy,
    GroupStrategy,
    MergeRoutingStrategy,
    NoStrategy,
    PaymentFormatAverageStrategy,
    ScatterGroupStrategy,
)

CONFIG_PATH = "./config.yaml"

@dataclass
class GroupConfig:
    mom_host: str
    input_exchange: str
    output_exchange: str
    log_level: str
    expected_eofs: int
    worker_id: int
    num_downstream_workers: int
    routing_strategy: str
    strategy: GroupStrategy

def _parse_strategy_config(raw_strategy: Any) -> GroupStrategy:
    strategy_type = _read_strategy_type(raw_strategy)

    if strategy_type == "BankMaxAmount":
        return BankMaxAmountStrategy()
    if strategy_type == "PaymentFormatAverage":
        return PaymentFormatAverageStrategy()
    if strategy_type == "AccountPairCount":
        return AccountPairCountStategy()
    if strategy_type == "MergeRouting":
        return MergeRoutingStrategy()
    if strategy_type == "Account":
        return AccountStrategy()
    if strategy_type == "ScatterGroup":
        return ScatterGroupStrategy()

    return NoStrategy()

def _read_strategy_type(raw_strategy: Any) -> str:
    if isinstance(raw_strategy, dict):
        return str(raw_strategy.get("type", "NoStrategy"))
    return str(raw_strategy or "NoStrategy")

def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}

def init_config() -> GroupConfig:
    file_config = _load_file_config()
    raw_strategy = file_config.get("strategy", "NoStrategy")

    return GroupConfig(
        mom_host=os.getenv("MOM_HOST", file_config.get("mom_host", "")),
        input_exchange=os.getenv("INPUT_EXCHANGE", file_config.get("input_exchange", "")),
        output_exchange=os.getenv("OUTPUT_EXCHANGE", file_config.get("output_exchange", "")),
        log_level=os.getenv("LOG_LEVEL", file_config.get("log_level", "INFO")),
        expected_eofs=int(os.getenv("EXPECTED_EOFS", file_config.get("expected_eofs", "1"))),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        num_downstream_workers=int(os.getenv("SHARD_AMOUNT", "1")),
        routing_strategy=os.getenv("ROUTING_STRATEGY", "sharded").lower(), 
        strategy=_parse_strategy_config(raw_strategy),
    )

def log_config(config: GroupConfig) -> None:
    logging.info(
        "Group startup with: mom_host=%s | input_exchange=%s | output_exchange=%s | "
        "expected_eofs=%d | strategy=%s",
        config.mom_host,
        config.input_exchange,
        config.output_exchange,
        config.expected_eofs,
        config.strategy,
    )

class GroupWorker(BaseWorker):
    """
    Stateless cross-batch grouping worker. 
    Uses strategies to calculate downstream routes.
    """
    def __init__(self, config: GroupConfig):
        super().__init__(config)

    def process_data(self, client_id: str, msg_id: str, payload: dict) -> None:
        batch = payload.get("batch", [])
        
        routed = self.strategy.group_and_route(batch)
        
        for route, grouped in routed:
            if not grouped:
                continue
            
            batch_msg = build_batch_message(
                message_type="batch",
                client=client_id,
                msg_id=str(uuid.uuid4()),
                batch=grouped,
            )
            
            self.send_downstream(client_id, batch_msg, shard_key=route)

    def flush_state(self, client_id: str) -> None:
        pass

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    worker = GroupWorker(config)

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        worker.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    
    worker.start()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
