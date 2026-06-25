import logging
import threading

# Ventana de gracia tras un restart del gateway: al volver no tiene sockets, asi
# que les da a los clientes persistidos este tiempo para reconectar antes de
# darlos por caidos. Con el gateway VIVO no hay gracia: la caida del socket se
# trata como muerte inmediata (un gateway vivo nunca ve reconectar a sus propios
# clientes; eso solo pasa si el gateway se cayo).
RECOVERY_GRACE_SECONDS = 30


class ClientReaper:
    """Detecta y declara la caida de clientes, de forma idempotente.

    Dos caminos de deteccion convergen aca:
    - Gateway vivo: ingress (recv falla) o egress (send falla) detectan la caida
      del socket y llaman directo a `declare_crashed`.
    - Tras restart del gateway: `arm_recovery_timers` arma un timer de gracia por
      cada cliente persistido; si el cliente reconecta (`note_reconnect`) se
      cancela, si expira se llama a `declare_crashed`.

    Al declarar caido se emite el wipe downstream (ver comentario en
    `declare_crashed`) y se borra TODO el estado durable del cliente. La
    completacion normal NO es una caida: usa `note_completed` (limpia estado
    local, sin wipe)."""

    def __init__(self, registry, router, state):
        self._registry = registry
        self._router = router
        self._state = state
        self._lock = threading.Lock()
        # client_id -> threading.Timer de gracia pendiente (solo post-restart).
        self._timers = {}
        # Clientes ya declarados caidos: guard de idempotencia para que ingress y
        # egress puedan llamar a la vez sin doble wipe/limpieza.
        self._dead = set()

    def arm_recovery_timers(self):
        """Arranca un timer de gracia por cada cliente persistido. Se llama una vez
        al iniciar el gateway, con el router ya construido."""
        client_ids = self._state.all_client_ids()
        with self._lock:
            for client_id in client_ids:
                self._arm_timer_locked(client_id)
        if client_ids:
            logging.info(
                "Armed %ds recovery grace timer for %d persisted client(s)",
                RECOVERY_GRACE_SECONDS,
                len(client_ids),
            )

    def note_reconnect(self, client_id):
        """El cliente reconecto: cancela su timer de gracia y lo "resucita" del set
        de caidos, asi una futura caida se vuelve a detectar."""
        with self._lock:
            self._cancel_timer_locked(client_id)
            self._dead.discard(client_id)

    def note_completed(self, client_id):
        """El cliente completo normalmente (se mando results_done): limpia su estado
        durable local. NO emite wipe (los workers se autolimpian con sus EOF)."""
        with self._lock:
            self._cancel_timer_locked(client_id)
        self._state.forget_client(client_id)
        logging.info("Client %s completed; durable state cleaned", client_id)

    def declare_crashed(self, client_id):
        """Declara caido al cliente: emite el wipe downstream y borra TODO su estado
        durable. Idempotente: solo actua la primera vez (ingress y egress pueden
        llamar a la vez)."""
        with self._lock:
            if client_id in self._dead:
                return
            self._dead.add(client_id)
            self._cancel_timer_locked(client_id)

        logging.warning("Client %s declared crashed; wiping downstream", client_id)

        # === WIPE DOWNSTREAM (NO IMPLEMENTADO) ===
        # Aca se debe emitir el mensaje especial a TODOS los workers (broadcast por
        # los 3 exchanges, como WorkerRouter.send_eof) para que borren toda la
        # informacion del cliente `client_id`. Se invoca SOLO al declarar que el
        # cliente se cayo:
        #   (1) gateway vivo: al detectar la caida del socket (ingress recv / egress
        #       send), o
        #   (2) tras restart del gateway: cuando el timer de 30s expira sin
        #       reconexion.
        # NO se emite en la finalizacion normal (ver note_completed).
        # self._router.send_client_wipe(client_id)

        self._registry.remove(client_id)
        self._state.forget_client(client_id)

    # ----- helpers internos (asumen self._lock tomado) -----

    def _arm_timer_locked(self, client_id):
        self._cancel_timer_locked(client_id)
        timer = threading.Timer(
            RECOVERY_GRACE_SECONDS, self._on_recovery_timeout, args=[client_id]
        )
        timer.daemon = True
        self._timers[client_id] = timer
        timer.start()

    def _cancel_timer_locked(self, client_id):
        timer = self._timers.pop(client_id, None)
        if timer is not None:
            timer.cancel()

    def _on_recovery_timeout(self, client_id):
        """Expiro la gracia sin que el cliente reconecte: se da por caido."""
        logging.info(
            "Recovery grace expired for client %s without reconnect", client_id
        )
        self.declare_crashed(client_id)
