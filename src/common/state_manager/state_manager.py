import os
import pickle
import logging
import glob
from typing import Any, Dict

class WorkerStateManager:
    """Handles atomic, per-client persistence of application state."""
    
    def __init__(self, base_dir: str, worker_id: int):
        self.base_dir = base_dir
        self.worker_id = worker_id
        # Ensure the directory exists
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_file_path(self, client_id: str) -> str:
        return os.path.join(self.base_dir, f"worker_{self.worker_id}_client_{client_id}.pkl")

    def load_all(self) -> Dict[str, Any]:
        """Scans the directory on boot and reconstructs the state for ALL clients."""
        full_state = {}
        # Find all files belonging to this specific worker
        search_pattern = os.path.join(self.base_dir, f"worker_{self.worker_id}_client_*.pkl")
        
        for file_path in glob.glob(search_pattern):
            # Extract client_id from the filename
            filename = os.path.basename(file_path)
            client_id = filename.replace(f"worker_{self.worker_id}_client_", "").replace(".pkl", "")
            
            try:
                with open(file_path, "rb") as f:
                    full_state[client_id] = pickle.load(f)
                logging.info("Recovered state for client %s", client_id)
            except Exception as e:
                logging.error("Failed to load state for client %s: %s", client_id, e)
                
        return full_state

    def save_client(self, client_id: str, client_state: Any) -> None:
        """Atomically saves ONLY the state for a single client."""
        file_path = self._get_file_path(client_id)
        temp_path = f"{file_path}.tmp"
        
        with open(temp_path, "wb") as f:
            pickle.dump(client_state, f)
            
        os.sync() 
        os.rename(temp_path, file_path)

    def delete_client(self, client_id: str) -> None:
        """Deletes the client's file from disk after EOF is processed."""
        file_path = self._get_file_path(client_id)
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info("Cleaned up persistent state for client %s", client_id)


import os
import pickle
import logging
import glob
import tempfile
from typing import Any, Dict

class WorkerStateManager:
    def __init__(self, base_dir: str, stage_name: str, worker_id: int):
        self.base_dir = base_dir
        self.stage_name = stage_name
        self.worker_id = worker_id
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_file_path(self, client_id: str) -> str:
        return os.path.join(self.base_dir, f"{self.stage_name}_{self.worker_id}_client_{client_id}.pkl")

    def load_all(self) -> Dict[str, Any]:
        full_state = {}
        search_pattern = os.path.join(self.base_dir, f"{self.stage_name}_{self.worker_id}_client_*.pkl")
        
        for file_path in glob.glob(search_pattern):
            filename = os.path.basename(file_path)
            prefix = f"{self.stage_name}_{self.worker_id}_client_"
            client_id = filename.replace(prefix, "").replace(".pkl", "")
            
            try:
                with open(file_path, "rb") as f:
                    full_state[client_id] = pickle.load(f)
                logging.info("Recovered state for client %s", client_id)
            except Exception as e:
                logging.error("Failed to load state for client %s: %s", client_id, e)
                
        return full_state

    def save_client(self, client_id: str, client_state: Any) -> None:
        file_path = self._get_file_path(client_id)
        
        fd, temp_path = tempfile.mkstemp(
            dir=self.base_dir, 
            prefix=f"tmp_{self.stage_name}_{self.worker_id}_{client_id}_", 
            suffix=".pkl"
        )
        
        try:
            with os.fdopen(fd, "wb") as f:
                pickle.dump(client_state, f)
                f.flush()
                os.fsync(f.fileno()) 
            
            os.chmod(temp_path, 0o644)
            os.rename(temp_path, file_path)
            
        except Exception as e:
            logging.error("Failed to save state for %s. Cleaning up temp file.", client_id)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise e 

    def delete_client(self, client_id: str) -> None:
        file_path = self._get_file_path(client_id)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except OSError as e:
            logging.error("Error removing state file %s: %s", file_path, e)