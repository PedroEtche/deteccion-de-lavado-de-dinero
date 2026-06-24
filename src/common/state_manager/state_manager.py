import os
import json
import logging
import tempfile
from typing import Any, Dict, List, Tuple
import glob
import dataclasses


def _custom_serializer(obj):
    """Fallback serializer for json.dump to handle custom dataclasses."""
    if dataclasses.is_dataclass(obj):
        return dataclasses.asdict(obj)

    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


class WorkerStateManager:
    def __init__(self, base_dir: str, stage_name: str, worker_id: int):
        self.base_dir = base_dir
        self.stage_name = stage_name
        self.worker_id = worker_id
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_snapshot_path(self, client_id: str) -> str:
        return os.path.join(
            self.base_dir, f"{self.stage_name}_{self.worker_id}_client_{client_id}.json"
        )

    def _get_wal_path(self, client_id: str) -> str:
        return os.path.join(
            self.base_dir,
            f"{self.stage_name}_{self.worker_id}_client_{client_id}.jsonl",
        )

    def append_batch(self, client_id: str, batch: Any, msg_id: int, sender: str) -> None:
        """Appends a batch to the client's WAL file. Each batch is a new line in JSON format."""
        if not batch:
            return

        record = {
            "msg_id": msg_id,
            "sender": sender,
            "batch": batch,
            }

        wal_path = self._get_wal_path(client_id)
        try:
            with open(wal_path, "a", encoding="utf-8") as f:
                json.dump(record, f, default=_custom_serializer)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logging.error("Failed to append to WAL for %s: %s", client_id, e)
            raise e

    def save_snapshot(
        self, client_id: str, client_state: Any, seen_msgs: Dict[str, int]
    ) -> None:
        """Atomically saves a snapshot of the client's state AND the per-sender
        seen_msgs, then empties the WAL. The seen_msgs have to travel with
        the state in the same atomic write — if they were saved separately,
        a crash between the two writes could leave a state snapshot that no
        longer matches the watermark that says "this state already accounts
        for messages up to here"."""
        if not client_state:
            return

        snapshot_path = self._get_snapshot_path(client_id)
        wal_path = self._get_wal_path(client_id)

        payload = {"state": client_state, "seen_msgs": seen_msgs}

        fd, temp_path = tempfile.mkstemp(
            dir=self.base_dir,
            prefix=f"tmp_snap_{self.stage_name}_{self.worker_id}_{client_id}_",
            suffix=".json",
        )

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, default=_custom_serializer)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, snapshot_path)
            open(wal_path, "w").close()
            logging.info(
                "Snapshot saved (seen_msgs=%s) and WAL truncated for %s",
                seen_msgs, client_id,
            )
        except Exception as e:
            logging.error("Failed to save snapshot for %s", client_id)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise e

    def recover_client(
        self, client_id: str
    ) -> Tuple[Dict[str, Any], List[Any], Dict[str, int]]:
        """Recovers (state, historical_batches, seen_msgs).
        seen_msgs: {sender: highest msg_id seen from that sender}.
        A parse failure on the WAL's last line is treated as an in-flight
        write interrupted by a crash and silently dropped; a parse failure
        on any earlier line means the single-writer-sequential-fsync
        guarantee was violated, which is a real error, not an expected one.
        """
        snapshot_path = self._get_snapshot_path(client_id)
        wal_path = self._get_wal_path(client_id)

        state: Dict[str, Any] = {}
        seen_msgs: Dict[str, int] = {}
        historical_batches: List[Any] = []

        if os.path.exists(snapshot_path):
            try:
                with open(snapshot_path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
                state = payload.get("state", {})
                seen_msgs = payload.get("seen_msgs", {})
            except Exception as e:
                logging.error("Corrupted snapshot for %s: %s", client_id, e)

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
                except json.JSONDecodeError:
                    if i == len(lines) - 1:
                        logging.info(
                            "Discarding torn trailing WAL record for %s", client_id
                        )
                        break
                    logging.error(
                        "WAL CORRUPTION (non-trailing) for %s, line %d", client_id, i
                    )
                    raise RuntimeError(
                        f"Corrupted WAL for client {client_id} at line {i}"
                    )

                sender = record["sender"]
                msg_id = record["msg_id"]
                seen_msgs[sender] = max(seen_msgs.get(sender, 0), msg_id)
                historical_batches.append(record["batch"])
                good_count += 1

            if good_count < len(lines):
                self._rewrite_wal_clean(wal_path, lines[:good_count])

        return state, historical_batches, seen_msgs

    def iter_wal_batches(self, client_id: str):
        """Itera el WAL del cliente batch por batch (una linea = un batch),
        sin cargar todo el archivo en memoria. Yields only the batch payload
        — sender/msg_id are dedup metadata, not data the caller needs here."""
        wal_path = self._get_wal_path(client_id)
        if not os.path.exists(wal_path):
            return
        with open(wal_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)["batch"]

    def _rewrite_wal_clean(self, wal_path: str, good_lines: List[str]) -> None:
        """Compacts the WAL down to its known-good lines after recovery
        discards a torn tail, so we don't re-discover and re-skip the same
        bad tail on every future restart."""
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
        for path in [self._get_snapshot_path(client_id), self._get_wal_path(client_id)]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError as e:
                logging.error("Error removing file %s: %s", path, e)

    def get_all_client_ids(self) -> set:
        """Scans the base directory for snapshot and WAL files to extract all unique client IDs."""
        client_ids = set()
        search_pattern = os.path.join(
            self.base_dir, f"{self.stage_name}_{self.worker_id}_client_*"
        )

        for file_path in glob.glob(search_pattern):
            filename = os.path.basename(file_path)
            prefix = f"{self.stage_name}_{self.worker_id}_client_"

            client_id = (
                filename.replace(prefix, "").replace(".jsonl", "").replace(".json", "")
            )
            client_ids.add(client_id)

        return client_ids
