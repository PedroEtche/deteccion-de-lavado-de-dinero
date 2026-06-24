import json
import logging
import os
import tempfile
import threading


class IngressCursorStore:
    """Cursor durable de ingreso por cliente: el ultimo msg_id que el gateway
    reenvio downstream.

    El cliente streamea sus batches (accounts + transactions) con un msg_id
    monotonico SIN esperar ACK por batch. El gateway reenvia cada uno aguas abajo
    y lleva este cursor; al reconectar, el cliente presenta su UUID y el gateway
    le responde desde donde retomar (cursor + 1).

    Invariante de correctitud: el valor persistido debe ser <= al ultimo msg_id
    REALMENTE reenviado downstream. Por eso el gateway persiste DESPUES de
    reenviar. Un cursor stale (se persistio de menos antes de una caida) solo
    provoca re-envio de batches ya vistos, que los workers deduplican por
    (client, sender, msg_id): nunca incorrectitud. Eso habilita persistir de
    forma perezosa (cada N mensajes + en el EOF), sin fsync por mensaje.

    Escritura atomica temp -> fsync -> os.replace (mismo idiom que UuidRegistry /
    GatewayResultProgress). Vive en /app/state para sobrevivir al `docker start`
    con que la deteccion de fallas revive un nodo.
    """

    def __init__(
        self,
        base_dir: str = "/app/state",
        filename: str = "gateway_ingress_cursor.json",
    ):
        self.base_dir = base_dir
        self.path = os.path.join(base_dir, filename)
        self._lock = threading.Lock()
        os.makedirs(self.base_dir, exist_ok=True)
        self._by_client = self._load()
        logging.info(
            "IngressCursorStore loaded cursor for %d client(s)", len(self._by_client)
        )

    def _load(self) -> dict:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return {client: int(v) for client, v in json.load(f).items()}
        except Exception as e:
            logging.error("Corrupted ingress cursor at %s: %s", self.path, e)
            return {}

    def _persist(self) -> None:
        snapshot = dict(self._by_client)
        fd, temp_path = tempfile.mkstemp(
            dir=self.base_dir, prefix="tmp_ingress_cursor_", suffix=".json"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(snapshot, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.path)
        except Exception as e:
            logging.error("Failed to persist ingress cursor: %s", e)
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def get(self, client_id: str) -> int:
        """Ultimo msg_id durable para el cliente, o -1 si no hay (cliente nuevo)."""
        with self._lock:
            return self._by_client.get(client_id, -1)

    def record(self, client_id: str, last_msg_id: int) -> None:
        """Persiste el cursor del cliente. Monotonico: nunca retrocede."""
        with self._lock:
            current = self._by_client.get(client_id, -1)
            if last_msg_id <= current:
                return
            self._by_client[client_id] = last_msg_id
            self._persist()
