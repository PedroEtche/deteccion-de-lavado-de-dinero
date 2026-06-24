import json
import logging
import os
import tempfile
import threading


class GatewayResultProgress:
    """Progreso durable de EOFs de resultado por cliente.

    Cada query que corre manda 1 EOF de resultado al terminar. El gateway debe
    contar esos EOFs por cliente para saber cuando completo (y recien ahi cerrar
    al cliente). Si ese conteo viviera solo en memoria, una caida del gateway lo
    perderia: al revivir arrancaria en 0 y, como RabbitMQ ya no reentrega los
    EOFs ya ackeados, nunca alcanzaria expected_results y el cliente quedaria
    colgado esperando.

    El estado es un set de tipos de resultado (qN_result) por
    cliente

    Vive en /app/state (igual que el estado de los workers y los UUIDs): asi
    sobrevive al `docker start` con que la deteccion de fallas revive un nodo.
    """

    def __init__(
        self,
        base_dir: str = "/app/state",
        filename: str = "gateway_result_progress.json",
    ):
        self.base_dir = base_dir
        self.path = os.path.join(base_dir, filename)
        self._lock = threading.Lock()
        os.makedirs(self.base_dir, exist_ok=True)
        self._by_client = self._load()
        logging.info(
            "GatewayResultProgress loaded progress for %d client(s)",
            len(self._by_client),
        )

    def _load(self) -> dict:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return {client: set(types) for client, types in json.load(f).items()}
        except Exception as e:
            logging.error("Corrupted result progress at %s: %s", self.path, e)
            return {}

    def _persist(self) -> None:
        snapshot = {client: sorted(types) for client, types in self._by_client.items()}
        fd, temp_path = tempfile.mkstemp(
            dir=self.base_dir, prefix="tmp_result_progress_", suffix=".json"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(snapshot, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.path)
        except Exception as e:
            logging.error("Failed to persist result progress: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def record(self, client_id: str, result_type: str) -> int:
        """Registra (idempotente) que llego el EOF de `result_type` para el
        cliente y persiste. Devuelve el conteo de tipos distintos vistos."""
        with self._lock:
            types = self._by_client.setdefault(client_id, set())
            if result_type not in types:
                types.add(result_type)
                self._persist()
            return len(types)

    def count(self, client_id: str) -> int:
        with self._lock:
            return len(self._by_client.get(client_id, set()))

    def is_complete(self, client_id: str, expected: int) -> bool:
        return self.count(client_id) >= expected
