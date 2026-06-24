import os
import json
import logging
import tempfile
import glob
import dataclasses
from typing import Any, Dict, List, Tuple

def _custom_serializer(obj):
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


class WorkerStateManager:
    def __init__(self, base_dir: str, stage_name: str, worker_id: int):
        self.base_dir = base_dir
        self.stage_name = stage_name
        self.worker_id = worker_id
        self.prefix = f"{stage_name}_{worker_id}_client_"
        os.makedirs(self.base_dir, exist_ok=True)
    
    def _get_path(self, client_id: str, suffix: str) -> str:
        return os.path.join(self.base_dir, f"{self.prefix}{client_id}{suffix}")

    def _read_json(self, path: str, default: Any) -> Any:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logging.error("Corrupted file %s: %s", path, e)
        return default

    def _atomic_write(self, target_path: str, payload: Any) -> None:
        """Lock-free atomic JSON write using an OS-level file swap."""
        fd, temp_path = tempfile.mkstemp(dir=self.base_dir, prefix="tmp_write_", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, default=_custom_serializer)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, target_path)
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise e


    def append_batch(self, client_id: str, batch: Any, msg_id: int, sender: str, msg_type: str = None) -> None:
        if not batch:
            return
        wal_path = self._get_path(client_id, ".jsonl")
        try:
            with open(wal_path, "a", encoding="utf-8") as f:
                json.dump({"msg_id": msg_id, "sender": sender, "msg_type": msg_type, "batch": batch}, f, default=_custom_serializer)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logging.error("Failed to append WAL for %s: %s", client_id, e)
            raise e

    def save_snapshot(self, client_id: str, client_state: Any, seen_msgs: Dict[str, int]) -> None:
        if not client_state: 
            return
        self._atomic_write(self._get_path(client_id, ".json"), {"state": client_state, "seen_msgs": seen_msgs})
        open(self._get_path(client_id, ".jsonl"), "w").close()  # Truncates WAL instantly
        logging.info("Snapshot saved and WAL truncated for %s", client_id)

    def save_results(self, client_id: str, results_data: list) -> None:
        self._atomic_write(self._get_path(client_id, "_results.json"), results_data)
        logging.info("Results securely committed to disk for %s", client_id)

    def load_results(self, client_id: str) -> list:
        return self._read_json(self._get_path(client_id, "_results.json"), [])

    def save_eof_count(self, client_id: str, eof_count: int) -> None:
        self._atomic_write(self._get_path(client_id, "_eof.json"), {"eof_count": eof_count})

    def load_eof_count(self, client_id: str) -> int:
        return self._read_json(self._get_path(client_id, "_eof.json"), {}).get("eof_count", 0)


    def iter_wal_batches(self, client_id: str):
        wal_path = self._get_path(client_id, ".jsonl")
        if not os.path.exists(wal_path): 
            return
        with open(wal_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)["batch"]

    def recover_client(self, client_id: str) -> Tuple[Dict[str, Any], List[Any], Dict[str, int]]:
        snap_data = self._read_json(self._get_path(client_id, ".json"), {})
        state = snap_data.get("state", {})
        seen_msgs = snap_data.get("seen_msgs", {})
        historical_batches = []

        wal_path = self._get_path(client_id, ".jsonl")
        if os.path.exists(wal_path):
            with open(wal_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            good_count = 0
            for i, line in enumerate(lines):
                if not line.strip():
                    good_count += 1
                    continue
                try:
                    record = json.loads(line)
                    sender, msg_id = record["sender"], record["msg_id"]
                    seen_msgs[sender] = max(seen_msgs.get(sender, 0), msg_id)
                    historical_batches.append((record["batch"], record.get("msg_type")))
                    good_count += 1
                except json.JSONDecodeError:
                    if i == len(lines) - 1:
                        logging.info("Discarding torn trailing WAL record for %s", client_id)
                        break
                    raise RuntimeError(f"Corrupted WAL for {client_id} at line {i}")

            if good_count < len(lines):
                self._rewrite_wal_clean(wal_path, lines[:good_count])

        return state, historical_batches, seen_msgs

    def _rewrite_wal_clean(self, wal_path: str, good_lines: List[str]) -> None:
        fd, temp_path = tempfile.mkstemp(dir=self.base_dir, suffix=".jsonl")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.writelines(good_lines)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, wal_path)
        except Exception:
            if os.path.exists(temp_path): 
                os.remove(temp_path)
            raise

    def delete_client(self, client_id: str) -> None:
        """Instantly wipes all files associated with this client via wildcard."""
        for path in glob.glob(self._get_path(client_id, "*")):
            try: 
                os.remove(path)
            except OSError as e: 
                logging.error("Error removing %s: %s", path, e)

    def get_all_client_ids(self) -> set:
        client_ids = set()
        for file_path in glob.glob(os.path.join(self.base_dir, f"{self.prefix}*")):
            filename = os.path.basename(file_path)
            if filename.startswith(self.prefix):
                # Isolate the client ID from suffixes like '123_eof.json'
                remainder = filename[len(self.prefix):]
                client_id = remainder.split('.')[0].replace('_eof', '').replace('_results', '')
                client_ids.add(client_id)
        return client_ids