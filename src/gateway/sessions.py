import threading
import uuid


class ClientSession:
    """Estado de una conexion de cliente: su socket y un Event que se setea
    cuando ya llegaron todos los EOF de resultado.

    El conteo de EOF de resultado NO vive aca: es durable y vive en
    GatewayResultProgress (sobrevive a una caida del gateway). La sesion solo
    refleja, via `done`, cuando el pipeline quedo completo."""

    def __init__(self, client_id, tcp):
        self.client_id = client_id
        self.tcp = tcp
        self.done = threading.Event()
        # Cursor de ingreso en memoria: ultimo msg_id reenviado downstream. Se
        # siembra en el handshake desde IngressCursorStore y avanza a medida que
        # el cliente streamea. El valor durable vive en el store.
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

    Seam de persistencia: `store` queda como punto de extension. Hoy es None
    (todo en memoria, comportamiento identico al original). Mas adelante se le
    inyecta un store basado en WorkerStateManager + DuplicateHandler sin tocar
    a los llamadores.

    """

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
            client_id = self._store.assign() if self._store else str(uuid.uuid4())
        else:
            client_id = presented_uuid
            if self._store is not None:
                self._store.register_existing(client_id)
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
