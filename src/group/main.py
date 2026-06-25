import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict

import yaml

from src.common.worker import StatelessWorker

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
    stage_name: str


def _extract_strategy_type(raw_strategy: Any) -> str:
    if isinstance(raw_strategy, dict):
        return str(raw_strategy.get("type", "NoStrategy"))
    return str(raw_strategy or "NoStrategy")


def _parse_strategy_config(raw_strategy: Any) -> GroupStrategy:
    strategy_type = _extract_strategy_type(raw_strategy)

    if strategy_type == "bank_max_amount":
        return BankMaxAmountStrategy()
    if strategy_type == "payment_format_average":
        return PaymentFormatAverageStrategy()
    if strategy_type == "account_pair_count":
        return AccountPairCountStategy()
    if strategy_type == "merge_routing":
        return MergeRoutingStrategy()
    if strategy_type == "account":
        return AccountStrategy()
    if strategy_type == "scatter_group":
        return ScatterGroupStrategy()

    return NoStrategy()


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def init_config() -> GroupConfig:
    data = _load_file_config()
    raw_strategy = data.get("strategy", "NoStrategy")

    return GroupConfig(
        mom_host=data.get("mom_host", "rabbitmq"),
        input_exchange=data.get("input", ""),
        output_exchange=data.get("output", ""),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        strategy=_parse_strategy_config(raw_strategy),
        expected_eofs=int(os.getenv("EOF_EXPECTED", "1")),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        num_downstream_workers=int(os.getenv("NUM_DOWNSTREAM_WORKERS", "1")),
        routing_strategy=os.getenv("ROUTING_STRATEGY", "round_robin").lower(),
        stage_name=os.getenv("STAGE_NAME", "group"),
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


class GroupWorker(StatelessWorker):
    """
    Stateless cross-batch grouping worker.
    Uses strategies to calculate downstream routes.
    """

    def __init__(self, config: GroupConfig):
        super().__init__(config)

    def process_batch(self, client_id: str, batch: list, msg_type: str, msg_id: int, sender: str) -> None:
        logging.info("Processing batch of %d rows for client %s", len(batch), client_id)
        routed = self.strategy.group_and_route(batch)
        # routed: [(clave_logica, batch), ...]; send_groups agrupa por worker fisico.
        self.send_groups(
            client_id, 
            routed, 
            msg_id=msg_id, 
            sender=sender
        )

    def flush_state(self, client_id: str) -> None:
        pass

    def clear_client_state(self, client_id: str) -> None:
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
