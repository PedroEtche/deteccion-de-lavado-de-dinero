import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict

import yaml

from src.common.worker import StatefulWorker
from src.common.state_manager import WorkerStateManager

from .strategies import (
    MergeStrategy,
    NoStrategy,
    AccountsStrategy,
    SelfMergeStrategy,
)

SNAPSHOT_BATCH = 100
RESULT_BATCH_SIZE = 5000

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
    stage_name: str


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
        routing_strategy=os.getenv("ROUTING_STRATEGY", "round_robin").lower(),
        stage_name=os.getenv("STAGE_NAME", "merge"),
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


class MergeWorker(StatefulWorker):
    """
    Stateful Merge. Uses a strategy to accumulate data in memory.
    Flushes state only when expected_eofs is reached.
    """

    def __init__(self, config: MergeConfig):
        super().__init__(config)

        self.received_batches_per_client = {}

        self.state_manager = WorkerStateManager(
            base_dir="/app/state",
            stage_name=config.stage_name,
            worker_id=config.worker_id,
        )

        client_ids = self.state_manager.get_all_client_ids()

        for client_id in client_ids:
            logging.info("Recovering state for client %s", client_id)
            self._recover_client_state(client_id)

    def _recover_client_state(self, client_id: str) -> None:
        pending_results = self.state_manager.load_results(client_id)
        if pending_results:
            logging.warning("Crash detected during previous flush! Resending atomic results for %s", client_id)
            self.execute_result(client_id, pending_results, "batch")
            
            self.strategy.clear_client_state(client_id)
            self.state_manager.delete_client(client_id)
            self.received_batches_per_client.pop(client_id, None)
            return

        snapshot, wal_batches, last_seen_msg = self.state_manager.recover_client(client_id)

        if snapshot:
            self.strategy.set_client_state(client_id, snapshot)

        self.duplicate_handler.restore_state(client_id, last_seen_msg)

        for batch, msg_type in wal_batches:
            self.strategy.merge_batch(batch, client_id, msg_type=msg_type or "batch")

        self.received_batches_per_client[client_id] = len(wal_batches)
        logging.info("Recovered client %s", client_id)

    def process_batch(self, client_id: str, batch: list, msg_type: str, msg_id: int, sender: str) -> None:
        count = self.received_batches_per_client.get(client_id, 0) + 1
        self.received_batches_per_client[client_id] = count

        self.state_manager.append_batch(client_id, batch, msg_id=msg_id, sender=sender, msg_type=msg_type)
        self.strategy.merge_batch(batch, client_id, msg_type)

        if count % SNAPSHOT_BATCH == 0:
            logging.info("Triggering checkpoint snapshot for client %s", client_id)

            current_state = self.strategy.get_client_state(client_id)
            current_last_seen_msg = self.duplicate_handler.get_state(client_id)
            self.state_manager.save_snapshot(client_id, current_state, current_last_seen_msg)

            self.received_batches_per_client[client_id] = 0

    def flush_state(self, client_id: str) -> None:
        logging.info(
            "All EOFs received. Flushing merge state for client %s", client_id
        )

        final_data = self.strategy.get_result_for_client(client_id)
        
        if not final_data:
                logging.info("No data to flush for client %s", client_id)
                self.strategy.clear_client_state(client_id)
                self.state_manager.delete_client(client_id)
                self.received_batches_per_client.pop(client_id, None)
                return

        dummy_group = [("merge_key", final_data)]
        results_to_send = self.prepare_results(dummy_group, RESULT_BATCH_SIZE)
        logging.info(
            "Prepared %d total records in %d chunks for client %s", 
            len(final_data), len(results_to_send), client_id
        )

        # atomic save of results in batches with msg_id
        self.state_manager.save_results(client_id, results_to_send)

        self.execute_result(client_id, results_to_send)
        self.clear_client_state(client_id)

    def clear_client_state(self, client_id):
        self.strategy.clear_client_state(client_id)
        self.state_manager.delete_client(client_id)
        self.received_batches_per_client.pop(client_id, None)

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

    worker.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
