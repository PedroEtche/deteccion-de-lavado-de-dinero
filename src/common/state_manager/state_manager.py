import os
import json
import logging
import tempfile
from typing import Any, Dict, List, Tuple
import glob

class WorkerStateManager:    
    def __init__(self, base_dir: str, stage_name: str, worker_id: int):
        self.base_dir = base_dir
        self.stage_name = stage_name
        self.worker_id = worker_id
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_snapshot_path(self, client_id: str) -> str:
        return os.path.join(self.base_dir, f"{self.stage_name}_{self.worker_id}_client_{client_id}.json")

    def _get_wal_path(self, client_id: str) -> str:
        return os.path.join(self.base_dir, f"{self.stage_name}_{self.worker_id}_client_{client_id}.jsonl")

    def append_batch(self, client_id: str, batch: Any) -> None:
        """Appends a batch to the client's WAL file. Each batch is a new line in JSON format."""
        if not batch: return
        
        wal_path = self._get_wal_path(client_id)
        try:
            with open(wal_path, "a", encoding="utf-8") as f:
                json.dump(batch, f)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            logging.error("Failed to append to WAL for %s: %s", client_id, e)
            raise e

    def save_snapshot(self, client_id: str, client_state: Any) -> None:
        """Atomically saves a snapshot of the client's state and empties the WAL."""
        if not client_state: return
            
        snapshot_path = self._get_snapshot_path(client_id)
        wal_path = self._get_wal_path(client_id)
        
        fd, temp_path = tempfile.mkstemp(
            dir=self.base_dir, 
            prefix=f"tmp_snap_{self.stage_name}_{self.worker_id}_{client_id}_", 
            suffix=".json"
        )
        
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(client_state, f)
                f.flush()
                os.fsync(f.fileno()) 
            os.replace(temp_path, snapshot_path)
            open(wal_path, 'w').close() 
            logging.info("Snapshot saved and WAL truncated for %s", client_id)
            
        except Exception as e:
            logging.error("Failed to save snapshot for %s", client_id)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise e 

    def recover_client(self, client_id: str) -> Tuple[Dict[str, Any], List[Any]]:
        """Recovers the client's state by loading the snapshot and the WAL."""
        snapshot_path = self._get_snapshot_path(client_id)
        wal_path = self._get_wal_path(client_id)
        
        snapshot = {}
        historical_batches = []

        if os.path.exists(snapshot_path):
            try:
                with open(snapshot_path, "r", encoding="utf-8") as f:
                    snapshot = json.load(f)
            except Exception as e:
                logging.error("Corrupted snapshot for %s: %s", client_id, e)

        if os.path.exists(wal_path):
            try:
                with open(wal_path, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            historical_batches.append(json.loads(line))
            except Exception as e:
                logging.error("Corrupted WAL for %s: %s", client_id, e)
                
        return snapshot, historical_batches

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
        search_pattern = os.path.join(self.base_dir, f"{self.stage_name}_{self.worker_id}_client_*")
        
        for file_path in glob.glob(search_pattern):
            filename = os.path.basename(file_path)
            prefix = f"{self.stage_name}_{self.worker_id}_client_"
            
            client_id = filename.replace(prefix, "").replace(".jsonl", "").replace(".json", "")
            client_ids.add(client_id)
            
        return client_ids