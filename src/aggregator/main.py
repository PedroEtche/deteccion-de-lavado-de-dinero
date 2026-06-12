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
    AccountPairCountStategy,
    AccountStrategy,
    AggregatorStrategy,
    BankMaxAmountStrategy,
    CountStrategy,
    NoStrategy,
    PaymentFormatAverageStrategy,
    ScatterAggregatorStrategy,
)

CONFIG_PATH = "./config.yaml"

@dataclass
class AggregatorConfig:
    mom_host: str
    input_exchange: str
    output_exchange: str
    log_level: str
    expected_eofs: int
    worker_id: int
    num_downstream_workers: int
    routing_strategy: str
    strategy: AggregatorStrategy

def _extract_strategy_type(raw_strategy) -> str:
    if isinstance(raw_strategy, dict):
        return str(raw_strategy.get("type", "NoStrategy"))
    return str(raw_strategy or "NoStrategy")

def _parse_strategy_config(raw_strategy) -> AggregatorStrategy:
    strategy_type = _extract_strategy_type(raw_strategy)

    if strategy_type == "bank_max_amount":
        return BankMaxAmountStrategy()
    if strategy_type == "account_pair_count":
        return AccountPairCountStategy()
    if strategy_type in ("payment_format_average"):
        return PaymentFormatAverageStrategy()
    if strategy_type == "account":
        return AccountStrategy()
    if strategy_type == "count":
        return CountStrategy()
    if strategy_type == "scatter_aggregator":
        return ScatterAggregatorStrategy()

    return NoStrategy()

def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}

def init_config() -> AggregatorConfig:
    data = _load_file_config()
    raw_strategy = data.get("strategy", "NoStrategy")

    return AggregatorConfig(
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

def log_config(config: AggregatorConfig) -> None:
    logging.info(
        "Aggregator startup with: mom_host=%s | input_exchange=%s | output_exchange=%s | "
        "worker_id=%d | expected_eofs=%d | strategy=%s",
        config.mom_host,
        config.input_exchange,
        config.output_exchange,
        config.worker_id,
        config.expected_eofs,
        config.strategy,
    )

class AggregatorWorker(BaseWorker):
    """
    Stateful aggregator. 
    Accumulates data in memory via strategies and flushes only upon receiving expected_eofs.
    """
    def __init__(self, config: AggregatorConfig) -> None:
        super().__init__(config)

    def process_data(self, client_id: str, msg_id: str, msg_type: str, payload: dict) -> None:
        batch = payload.get("batch", [])
        self.strategy.aggregate_batch(batch, client_id)

    def flush_state(self, client_id: str) -> None:
        logging.info("All EOFs received. Flushing aggregated result for client %s", client_id)
        routed_batches = self.strategy.get_result_for_client(client_id)
        if not routed_batches:
            self.strategy.clear_client_state(client_id)
            return

        if self.config.routing_strategy == "sharded":
            physical_groups: dict[str, list] = {}
            
            for logical_key, batch in routed_batches:
                routing_key = self.get_sharded_route(str(logical_key))
                
                if routing_key not in physical_groups:
                    physical_groups[routing_key] = []
                physical_groups[routing_key].extend(batch)

            for routing_key, combined_batch in physical_groups.items():
                batch_msg = build_batch_message(
                    message_type="batch",
                    client=client_id,
                    msg_id=str(uuid.uuid4()),
                    batch=combined_batch,
                )
                self.send_downstream(client_id, batch_msg, shard_routing_key=routing_key)
                
        else:
            logging.info("routed: %s", routed_batches)
            flat_batch = []
            for _, batch in routed_batches:
                flat_batch.extend(batch)
            
            batch_msg = build_batch_message(
                message_type="batch",
                client=client_id,
                msg_id=str(uuid.uuid4()),
                batch=flat_batch,
            )
            self.send_downstream(client_id, batch_msg)

        self.strategy.clear_client_state(client_id)

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    worker = AggregatorWorker(config)

    def handle_sigterm(_signum, _frame):
        logging.info("Received SIGTERM signal")
        worker.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)
    
    worker.start()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
