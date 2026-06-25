import logging
import os
import signal
from dataclasses import dataclass
from typing import Any, Dict, List

import yaml

from src.common.worker import StatefulWorker
from src.common.state_manager import WorkerStateManager

from .strategies import (
    AveragesUnionStrategy,
    CountStrategy,
    JoinStrategy,
    NoStrategy,
    QueryResultStrategy,
)

SNAPSHOT_BATCH = 100

CONFIG_PATH = "./config.yaml"


@dataclass
class JoinConfig:
    mom_host: str
    input_exchange: str
    output_exchange: str
    log_level: str
    strategy: JoinStrategy
    expected_eofs: int
    worker_id: int
    num_downstream_workers: int
    routing_strategy: str
    stage_name: str


def _load_file_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def _build_strategy(
    strategy_data: List[Dict[str, Any]], query_result_number: str = None
) -> JoinStrategy:
    params: Dict[str, Any] = {}
    for item in strategy_data:
        params.update(item)

    strategy_type = params.get("type", "none")

    _BUILDERS = {
        "none": lambda _: NoStrategy(),
        "query_result": lambda _: QueryResultStrategy(query_result_number),
        "count": lambda _: CountStrategy(),
        "averages_union": lambda _: AveragesUnionStrategy(),
    }

    builder = _BUILDERS.get(strategy_type)
    if builder is None:
        raise ValueError(f"Unknown strategy type: {strategy_type!r}")
    return builder(params)


def init_config() -> JoinConfig:
    data = _load_file_config()
    query_result_number = os.getenv("QUERY_RESULT_NUMBER", "1").lower()
    return JoinConfig(
        mom_host=data.get("mom_host", "rabbitmq"),
        input_exchange=data.get("input", ""),
        output_exchange=data.get("output", ""),
        log_level=os.environ.get("LOG_LEVEL", "INFO"),
        strategy=_build_strategy(
            data.get("strategy", []), query_result_number=query_result_number
        ),
        expected_eofs=int(os.getenv("EOF_EXPECTED", "1")),
        worker_id=int(os.getenv("WORKER_ID", "1")),
        num_downstream_workers=int(os.getenv("NUM_DOWNSTREAM_WORKERS", "1")),
        routing_strategy=os.getenv("ROUTING_STRATEGY", "round_robin").lower(),
        stage_name=os.getenv("STAGE_NAME", "join"),
    )


def log_config(config: JoinConfig) -> None:
    logging.info(
        "Join startup with: mom_host=%s | input_exchange=%s | output_exchange=%s | strategy=%s | expected_eofs=%d",
        config.mom_host,
        config.input_exchange,
        config.output_exchange,
        str(config.strategy),
        config.expected_eofs,
    ) 


class JoinWorker(StatefulWorker):
    def __init__(self, config: JoinConfig):
        super().__init__(config)
        self.strategy = config.strategy
        self.strategy.register_join_callback(self._join_callback)

        self._is_recovering = False

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

    def _join_callback(self, client_id: str, message: dict) -> None:
        if not message:
            return
        
        if self._is_recovering:
            return

        msg_id = self._next_msg_id()
        message["msg_id"] = msg_id
        message["sender"] = self.sender_id
        
        self._route_and_send(client_id, message, msg_id)

    def _recover_client_state(self, client_id: str) -> None:
        pending_results = self.state_manager.load_results(client_id)
        if pending_results:
            logging.warning("Crash detected during previous flush! Resending atomic results for %s", client_id)
            self._execute_join_results(client_id, pending_results)
            
            self.state_manager.delete_client(client_id)
            self.received_batches_per_client.pop(client_id, None)
            return

        snapshot, wal_batches, last_seen_msg = self.state_manager.recover_client(client_id)

        if snapshot:
            self.strategy.set_client_state(client_id, snapshot)

        self.duplicate_handler.restore_state(client_id, last_seen_msg)

        self._is_recovering = True
        for batch, _msg_type in wal_batches:
            self.strategy.join_batch(batch, client_id)
        self._is_recovering = False

        self.received_batches_per_client[client_id] = len(wal_batches)
        logging.info("Recovered client %s with %d WAL batches", client_id, len(wal_batches))

    def process_batch(self, client_id: str, batch: list, msg_type: str, msg_id: int, sender: str) -> None:
        count = self.received_batches_per_client.get(client_id, 0) + 1
        self.received_batches_per_client[client_id] = count

        self.state_manager.append_batch(client_id, batch, msg_id=msg_id, sender=sender)
        self.strategy.join_batch(batch, client_id)

        if count % SNAPSHOT_BATCH == 0:
            logging.info("Triggering checkpoint snapshot for client %s", client_id)

            current_state = self.strategy.get_client_state(client_id)
            if current_state is not None:
                current_last_seen_msg = self.duplicate_handler.get_state(client_id)
                self.state_manager.save_snapshot(client_id, current_state, current_last_seen_msg)
                self.received_batches_per_client[client_id] = 0

    def flush_state(self, client_id: str) -> None:
        final_msg = self.strategy.flush(client_id)
        
        if not final_msg:
            self.state_manager.delete_client(client_id)
            self.received_batches_per_client.pop(client_id, None)
            return

        final_msg["msg_id"] = self._next_msg_id()
        final_msg["sender"] = self.sender_id

        self.state_manager.save_results(client_id, [final_msg])

        self._execute_join_results(client_id, [final_msg])

        self.state_manager.delete_client(client_id)
        self.received_batches_per_client.pop(client_id, None)
        
    def _execute_join_results(self, client_id: str, results: list) -> None:
        """Translates the saved outbox dictionaries into messages directly."""
        for message in results:
            self._route_and_send(client_id, message, msg_id=message["msg_id"])

    def clear_client_state(self, client_id: str) -> None:
        self.strategy.clear_client_state(client_id)
        self.state_manager.delete_client(client_id)
        self.received_batches_per_client.pop(client_id, None)

def main() -> int:
    config = init_config()
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    log_config(config)
    logging.getLogger("pika").setLevel(logging.WARNING)

    worker = JoinWorker(config)

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        worker.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    worker.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
