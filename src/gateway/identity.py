import json
import logging
import os
import tempfile
import threading
import uuid as uuid_lib


class UuidRegistry:
    """Set durable de UUIDs de cliente asignados por el gateway.

    El gateway es la autoridad de identidad: cuando un cliente conecta sin id le
    asigna uno; cuando reconecta con su id, lo reconoce. El set se persiste a
    disco con escritura atomica (temp -> fsync -> os.replace, mismo idiom que
    WorkerStateManager.save_snapshot) para sobrevivir tambien a una caida del
    gateway: al revivir, se recarga del disco.

    Vive en /app/state dentro del contenedor (igual que el estado de los
    workers): sobrevive al `docker start` con que la deteccion de fallas revive
    un nodo.
    """

    def __init__(self, base_dir: str = "/app/state", filename: str = "gateway_uuids.json"):
        self.base_dir = base_dir
        self.path = os.path.join(base_dir, filename)
        self._lock = threading.Lock()
        os.makedirs(self.base_dir, exist_ok=True)
        self._uuids = self._load()
        logging.info("UuidRegistry loaded %d known client UUID(s)", len(self._uuids))

    def _load(self) -> set:
        if not os.path.exists(self.path):
            return set()
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception as e:
            logging.error("Corrupted UUID registry at %s: %s", self.path, e)
            return set()

    def _persist(self) -> None:
        fd, temp_path = tempfile.mkstemp(
            dir=self.base_dir, prefix="tmp_uuids_", suffix=".json"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(sorted(self._uuids), f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.path)
        except Exception as e:
            logging.error("Failed to persist UUID registry: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def assign(self) -> str:
        """Genera y persiste un UUID nuevo (no usado) y lo devuelve."""
        with self._lock:
            new_uuid = str(uuid_lib.uuid4())
            while new_uuid in self._uuids:
                new_uuid = str(uuid_lib.uuid4())
            self._uuids.add(new_uuid)
            self._persist()
            return new_uuid

    def register_existing(self, client_uuid: str) -> None:
        """Registra un UUID que el cliente presento (reanudacion). Si no estaba,
        lo agrega y persiste, logueando un warning: deberia haberlo asignado el
        gateway antes, asi que un id desconocido es anomalo (registro perdido o
        cliente con estado de otra corrida)."""
        with self._lock:
            if client_uuid in self._uuids:
                return
            logging.warning(
                "Client presented unknown UUID %s; registering it", client_uuid
            )
            self._uuids.add(client_uuid)
            self._persist()

    def knows(self, client_uuid: str) -> bool:
        with self._lock:
            return client_uuid in self._uuids
