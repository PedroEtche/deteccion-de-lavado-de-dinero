import json
import logging
import os
import tempfile
import threading
import uuid


def _atomic_write_json(base_dir: str, path: str, obj, tmp_prefix: str) -> None:
    """Escribe `obj` como JSON en `path` de forma atomica: temp -> fsync ->
    os.replace (mismo idiom que WorkerStateManager.save_snapshot). Limpia el temp
    ante error. Un lector nunca ve un archivo a medio escribir."""
    fd, temp_path = tempfile.mkstemp(dir=base_dir, prefix=tmp_prefix, suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, path)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise


def _load_json(path: str, default):
    """Carga un JSON o devuelve `default` si no existe o esta corrupto."""
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error("Corrupted gateway state at %s: %s", path, e)
        return default


class GatewayState:
    """Estado durable del gateway por cliente, reunido en una sola clase.

    Agrupa las tres piezas de estado que el gateway debe sobrevivir a su propia
    caida (cada una en su propio archivo dentro de /app/state, con escritura
    atomica):

    - UUIDs asignados: el gateway es la autoridad de identidad. Asigna un id al
      cliente nuevo y lo reconoce al reconectar.
    - Cursor de ingreso (uuid -> ultimo msg_id reenviado downstream): base de la
      reanudacion del streaming de datos. Invariante: el valor persistido debe ser
      <= al ultimo msg_id REALMENTE reenviado, por eso se persiste DESPUES de
      reenviar. Un cursor stale solo provoca re-envio (deduplicado aguas abajo),
      nunca incorrectitud; eso habilita persistir de forma perezosa.
    - Progreso de EOFs de resultado (uuid -> set de tipos qN_result vistos): el
      gateway cuenta los EOF por cliente para saber cuando completo y cerrarlo. Si
      viviera solo en memoria, una caida lo perderia y el cliente quedaria colgado
      (RabbitMQ ya no reentrega los EOF ackeados).

    Un unico lock serializa las (poco frecuentes y cortas) escrituras a disco.
    """

    def __init__(self, base_dir: str = "/app/state"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self._lock = threading.Lock()

        self._uuids_path = os.path.join(base_dir, "gateway_uuids.json")
        self._cursor_path = os.path.join(base_dir, "gateway_ingress_cursor.json")
        self._result_path = os.path.join(base_dir, "gateway_result_progress.json")

        self._uuids = set(_load_json(self._uuids_path, []))
        self._cursor_by_client = {
            c: int(v) for c, v in _load_json(self._cursor_path, {}).items()
        }
        self._result_by_client = {
            c: set(types) for c, types in _load_json(self._result_path, {}).items()
        }
        logging.info(
            "GatewayState loaded: %d uuid(s), cursor for %d client(s), "
            "result progress for %d client(s)",
            len(self._uuids),
            len(self._cursor_by_client),
            len(self._result_by_client),
        )

    # ----- identidad (UUIDs) -----

    def assign_uuid(self) -> str:
        """Genera y persiste un UUID nuevo (no usado) y lo devuelve."""
        with self._lock:
            new_uuid = str(uuid.uuid4())
            while new_uuid in self._uuids:
                new_uuid = str(uuid.uuid4())
            self._uuids.add(new_uuid)
            _atomic_write_json(
                self.base_dir, self._uuids_path, sorted(self._uuids), "tmp_uuids_"
            )
            return new_uuid

    def register_uuid(self, client_uuid: str) -> None:
        """Registra un UUID que el cliente presento (reanudacion). Si no estaba, lo
        agrega y persiste, logueando un warning: el gateway deberia haberlo asignado
        antes, asi que un id desconocido es anomalo (registro perdido o cliente con
        estado de otra corrida)."""
        with self._lock:
            if client_uuid in self._uuids:
                return
            logging.warning(
                "Client presented unknown UUID %s; registering it", client_uuid
            )
            self._uuids.add(client_uuid)
            _atomic_write_json(
                self.base_dir, self._uuids_path, sorted(self._uuids), "tmp_uuids_"
            )

    def knows_uuid(self, client_uuid: str) -> bool:
        with self._lock:
            return client_uuid in self._uuids

    def all_client_ids(self) -> list:
        """Lista de UUIDs persistidos (autoridad de identidad durable). La usa el
        reaper al arrancar para armar un timer de gracia por cada cliente conocido."""
        with self._lock:
            return sorted(self._uuids)

    def forget_client(self, client_id: str) -> None:
        """Borra TODO el estado durable del cliente (UUID, cursor de ingreso y
        progreso de EOFs de resultado) y reescribe solo los archivos que cambiaron.
        Se invoca al declarar caido a un cliente o al limpiarlo tras completar."""
        with self._lock:
            if client_id in self._uuids:
                self._uuids.discard(client_id)
                _atomic_write_json(
                    self.base_dir, self._uuids_path, sorted(self._uuids), "tmp_uuids_"
                )
            if client_id in self._cursor_by_client:
                del self._cursor_by_client[client_id]
                _atomic_write_json(
                    self.base_dir,
                    self._cursor_path,
                    dict(self._cursor_by_client),
                    "tmp_ingress_cursor_",
                )
            if client_id in self._result_by_client:
                del self._result_by_client[client_id]
                snapshot = {c: sorted(t) for c, t in self._result_by_client.items()}
                _atomic_write_json(
                    self.base_dir, self._result_path, snapshot, "tmp_result_progress_"
                )

    # ----- cursor de ingreso -----

    def cursor_get(self, client_id: str) -> int:
        """Ultimo msg_id durable para el cliente, o -1 si no hay (cliente nuevo)."""
        with self._lock:
            return self._cursor_by_client.get(client_id, -1)

    def cursor_record(self, client_id: str, last_msg_id: int) -> None:
        """Persiste el cursor del cliente. Monotonico: nunca retrocede."""
        with self._lock:
            if last_msg_id <= self._cursor_by_client.get(client_id, -1):
                return
            self._cursor_by_client[client_id] = last_msg_id
            _atomic_write_json(
                self.base_dir,
                self._cursor_path,
                dict(self._cursor_by_client),
                "tmp_ingress_cursor_",
            )

    # ----- progreso de EOFs de resultado -----

    def result_record(self, client_id: str, result_type: str) -> int:
        """Registra (idempotente) que llego el EOF de `result_type` para el cliente
        y persiste. Devuelve el conteo de tipos distintos vistos."""
        with self._lock:
            types = self._result_by_client.setdefault(client_id, set())
            if result_type not in types:
                types.add(result_type)
                snapshot = {c: sorted(t) for c, t in self._result_by_client.items()}
                _atomic_write_json(
                    self.base_dir, self._result_path, snapshot, "tmp_result_progress_"
                )
            return len(types)

    def result_count(self, client_id: str) -> int:
        with self._lock:
            return len(self._result_by_client.get(client_id, set()))

    def results_complete(self, client_id: str, expected: int) -> bool:
        # No sostiene el lock: delega en result_count (que si lo toma) para evitar
        # re-entrancia del lock no-reentrante.
        return self.result_count(client_id) >= expected


class ClientSession:
    """Estado de una conexion de cliente: su socket y un Event que se setea
    cuando ya llegaron todos los EOF de resultado.

    El conteo de EOF de resultado NO vive aca: es durable y vive en GatewayState
    (sobrevive a una caida del gateway). La sesion solo refleja, via `done`, cuando
    el pipeline quedo completo."""

    def __init__(self, client_id, tcp):
        self.client_id = client_id
        self.tcp = tcp
        self.done = threading.Event()
        # Cursor de ingreso en memoria: ultimo msg_id reenviado downstream. Se
        # siembra en el handshake desde GatewayState (cursor_get) y avanza a medida
        # que el cliente streamea. El valor durable vive en GatewayState.
        self.last_msg_id = -1
        # Dos caminos escriben al socket del cliente: los ACKs (thread de
        # ingress) y los resultados (thread de egress). El lock serializa los
        # envios para no entrelazar frames.
        self._send_lock = threading.Lock()

    def attach_socket(self, tcp):
        """Reusa la sesion con un socket nuevo (base de la reconexion)."""
        with self._send_lock:
            self.tcp = tcp

    def send(self, payload: bytes):
        """Envia bytes al cliente de forma thread-safe sobre el socket actual."""
        with self._send_lock:
            self.tcp.send_bytes(payload)


class ClientRegistry:
    """Registro thread-safe de sesiones de cliente.

    Las sesiones viven en memoria (se rearman al reconectar). La identidad durable
    la provee `store` (un GatewayState): asigna UUIDs nuevos y reconoce los que el
    cliente presenta al reanudar. Si `store` es None, los UUIDs se generan en
    memoria (sin durabilidad), util para tests."""

    def __init__(self, store=None):
        self._lock = threading.Lock()
        self._sessions = {}
        self._store = store

    def register(self, tcp, client_id=None):
        with self._lock:
            if client_id is not None and client_id in self._sessions:
                session = self._sessions[client_id]
                session.attach_socket(tcp)
                return session

            if client_id is None:
                client_id = str(uuid.uuid4())

            session = ClientSession(client_id, tcp)
            self._sessions[client_id] = session
            return session

    def handshake(self, tcp, presented_uuid=None):
        """Resuelve la identidad de un cliente que conecta y devuelve su sesion.

        - Sin `presented_uuid`: el store asigna un UUID nuevo (durable).
        - Con `presented_uuid` (reanudacion): se registra en el store si no
          estaba y se reusa/crea la sesion bajo ese id.

        El UUID resuelto queda en `session.client_id`."""
        if presented_uuid is None:
            client_id = self._store.assign_uuid() if self._store else str(uuid.uuid4())
        else:
            client_id = presented_uuid
            if self._store is not None:
                self._store.register_uuid(client_id)
        return self.register(tcp, client_id=client_id)

    def get(self, client_id):
        with self._lock:
            return self._sessions.get(client_id)

    def remove(self, client_id, tcp=None):
        """Quita la sesion. Si se pasa `tcp`, solo la quita cuando sigue atada a
        ese socket: evita que un handler viejo (cuyo socket murio) borre una
        sesion que un handler nuevo ya reattacheo tras una reconexion."""
        with self._lock:
            session = self._sessions.get(client_id)
            if session is None:
                return
            if tcp is not None and session.tcp is not tcp:
                return
            self._sessions.pop(client_id, None)

    def active_ids(self):
        with self._lock:
            return list(self._sessions.keys())
