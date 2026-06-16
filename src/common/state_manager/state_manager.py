import os
import json
import logging
import glob
from typing import Any, Dict, List

class WorkerStateManager:
    """Handles Append-Only (WAL) persistence using JSON Lines (.jsonl)."""
    
    def __init__(self, base_dir: str, stage_name: str, worker_id: int):
        self.base_dir = base_dir
        self.stage_name = stage_name
        self.worker_id = worker_id
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_file_path(self, client_id: str) -> str:
        return os.path.join(self.base_dir, f"{self.stage_name}_{self.worker_id}_client_{client_id}.jsonl")

    def load_all(self) -> Dict[str, List[Any]]:
        full_state = {}
        search_pattern = os.path.join(self.base_dir, f"{self.stage_name}_{self.worker_id}_client_*.jsonl")
        
        for file_path in glob.glob(search_pattern):
            filename = os.path.basename(file_path)
            prefix = f"{self.stage_name}_{self.worker_id}_client_"
            client_id = filename.replace(prefix, "").replace(".jsonl", "")
            
            try:
                history = []
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            history.append(json.loads(line))
                            
                full_state[client_id] = history
                logging.info("Recovered %d state deltas for client %s", len(history), client_id)
            except Exception as e:
                logging.error("Failed to load state for client %s: %s", client_id, e)
                
        return full_state

    def save_client(self, client_id: str, client_delta: Any) -> None:
        """Appends a new JSON string as a new line in the file."""
        if client_delta is None:
            return
            
        file_path = self._get_file_path(client_id)
        
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                serialized_data = json.dumps(client_delta)
                
                f.write(serialized_data + "\n")
                
                f.flush()
                os.fsync(f.fileno()) 
                
        except Exception as e:
            logging.error("Failed to append state delta for %s", client_id)
            raise e 

    def delete_client(self, client_id: str) -> None:
        file_path = self._get_file_path(client_id)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except OSError as e:
            logging.error("Error removing state file %s: %s", file_path, e)