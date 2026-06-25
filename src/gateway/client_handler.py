import logging
import socket
import threading
import time

from src.common.communication.internal import (
    build_ack_message,
    build_hello_ack_message,
    build_results_done_message,
    deserialize,
    serialize,
)
from src.common.communication.tcp import TCPSocket

# Cada cuantos mensajes de datos se persiste el cursor de ingreso. Es solo una
# palanca de performance: persistir de menos solo provoca re-envio (deduplicado
# aguas abajo) tras una caida, nunca perdida. El EOF persiste siempre.
PERSIST_EVERY = 500

# Espera corta antes de reencolar un resultado que todavia no se pudo entregar
# (cliente desconectado). Con prefetch=1 el broker reentrega de inmediato; sin
# esta pausa el consumidor spinnea a full hasta que el cliente reconecte.
_REQUEUE_BACKOFF = 0.5


class WorkerRouter:
    """Rutea los mensajes de salida del gateway hacia los workers.

    - accounts: broadcast a worker_1..N
    - transactions: round-robin determinista por msg_id (a date y a usd)
    - eof: broadcast a "eof_broadcast" en los 3 exchanges

    El round-robin usa `(msg_id % N) + 1` (mismo esquema que BaseWorker): sin
    contadores ni estado propio, y una reentrega del mismo msg_id cae siempre
    en el mismo worker. El _lock protege los channels de pika (no son
    thread-safe) frente a los multiples threads de cliente que publican a la vez.
    """

    def __init__(
        self,
        transactions_usd_mw,
        transactions_date_mw,
        accounts_mw,
        transactions_usd_workers,
        transactions_date_workers,
        accounts_workers,
    ):
        self.transactions_usd_mw = transactions_usd_mw
        self.transactions_date_mw = transactions_date_mw
        self.accounts_mw = accounts_mw
        self.transactions_usd_workers = transactions_usd_workers
        self.transactions_date_workers = transactions_date_workers
        self.accounts_workers = accounts_workers
        self._lock = threading.Lock()

    def send_accounts(self, serialized_message: bytes):
        """Broadcasts data to accounts workers."""
        with self._lock:
            logging.info(
                "Broadcasting accounts message to %d workers", self.accounts_workers
            )
            for worker_id in range(1, self.accounts_workers + 1):
                self.accounts_mw.send(
                    serialized_message,
                    routing_key=f"worker_{worker_id}",
                )

    def send_transactions(self, serialized_message: bytes, msg_id: int):
        """Sends data via deterministic Round-Robin (msg_id % N) to a worker."""
        date_worker = (msg_id % self.transactions_date_workers) + 1
        usd_worker = (msg_id % self.transactions_usd_workers) + 1
        with self._lock:
            logging.info(
                "Routing transactions message to workers with Round-Robin strategy"
            )
            self.transactions_date_mw.send(
                serialized_message,
                routing_key=f"worker_{date_worker}",
            )
            self.transactions_usd_mw.send(
                serialized_message,
                routing_key=f"worker_{usd_worker}",
            )

    def send_eof(self, eof_message: bytes):
        """Broadcasts EOF to all workers listening to the exchange."""
        routing_key = "eof_broadcast"
        with self._lock:
            self.transactions_usd_mw.send(eof_message, routing_key=routing_key)
            self.transactions_date_mw.send(eof_message, routing_key=routing_key)
            self.accounts_mw.send(eof_message, routing_key=routing_key)


class ClientHandler:
    """Maneja una conexion de cliente: lee mensajes, los despacha al pipeline y
    espera a que vuelvan todos los resultados antes de cerrar la conexion.

    Entrega de datos (streaming): el cliente blastea sus batches con un msg_id
    monotonico SIN ACK por batch. El gateway reenvia cada uno downstream
    (reutilizando el msg_id del cliente) y lleva un cursor durable del ultimo
    msg_id reenviado. Al reconectar, le responde al cliente desde donde retomar
    (ver GatewayState.cursor_get). El EOF si es stop-and-wait: barrera + flush.
    """

    def __init__(
        self,
        client_socket,
        registry,
        router,
        sender_id,
        shutdown_event,
        state,
        expected_results,
        reaper,
    ):
        self.tcp = TCPSocket(client_socket)
        self.registry = registry
        self.router = router
        self.sender_id = sender_id
        self._shutdown = shutdown_event
        # Declara la caida del cliente (wipe downstream + limpieza de estado) cuando
        # se detecta que el socket murio con el gateway vivo.
        self.reaper = reaper
        # Estado durable del gateway: cursor de ingreso (reanudacion del stream) y
        # progreso de EOFs de resultado (permite cerrar a un cliente que, tras un
        # restart del gateway, reconecta y reenvia su EOF cuando los EOFs de
        # resultado ya estaban todos persistidos).
        self.state = state
        self.expected_results = expected_results
        self._handlers = {
            "raw_transactions": self._on_transactions,
            "raw_accounts": self._on_accounts,
            "eof": self._on_eof,
        }

    def run(self):
        session = None
        client_id = None

        try:
            # El primer mensaje es siempre el handshake de identificacion. El
            # gateway resuelve el UUID (lo asigna si el cliente no traia uno) y
            # se lo confirma; recien despues empieza el flujo de datos.
            session, client_id = self._do_handshake()
            if session is None:
                return

            while not self._shutdown.is_set():
                try:
                    raw_bytes = self.tcp.recv_bytes()
                except ConnectionError:
                    # Con el gateway vivo, una caida del socket en la fase de datos
                    # solo puede ser la muerte del cliente: lo damos por caido
                    # (wipe + limpieza) y no esperamos resultados. En shutdown
                    # ordenado salimos sin declarar caida.
                    if self._shutdown.is_set():
                        break
                    logging.info("The client connection was closed; declaring crash")
                    self.reaper.declare_crashed(client_id)
                    return

                message = deserialize(raw_bytes)
                msg_type = message.get("type")
                handler = self._handlers.get(msg_type)
                if handler is not None and handler(client_id, message, session):
                    break

            session.done.wait()
            logging.info(
                "Pipeline complete for client %s; closing client socket", client_id
            )
            # Avisamos al cliente que la sesion quedo completa antes de cerrar:
            # si reconecto en la fase de resultados, esto lo hace cortar limpio
            # en vez de reintentar (best-effort: el socket puede ya estar muerto).
            try:
                session.send(
                    serialize(
                        build_results_done_message(
                            client=client_id, sender=self.sender_id
                        )
                    )
                )
            except Exception:
                pass

            # Completacion normal: limpiamos el estado durable del cliente. NO se
            # emite wipe downstream (los workers se autolimpian con sus EOF).
            self.reaper.note_completed(client_id)

        except socket.error:
            logging.error("The connection with the server was lost")
        except Exception as e:
            logging.error(e)
        finally:
            if client_id is not None:
                self.registry.remove(client_id, self.tcp)
            self.tcp.close()

    def _do_handshake(self):
        """Lee el `hello`, resuelve la sesion/UUID y responde `hello_ack`.
        Devuelve (session, client_id), o (None, None) si la conexion se corto o
        el primer mensaje no fue un `hello`."""
        try:
            message = deserialize(self.tcp.recv_bytes())
        except ConnectionError:
            logging.info("Client disconnected before handshake")
            return None, None

        if message.get("type") != "hello":
            logging.error(
                "Expected hello as first message, got %s", message.get("type")
            )
            return None, None

        presented = message.get("client")
        session = self.registry.handshake(self.tcp, presented_uuid=presented)
        client_id = session.client_id
        # Reconecto: si habia un timer de gracia post-restart corriendo para este
        # cliente, lo cancelamos (ya no esta caido).
        self.reaper.note_reconnect(client_id)
        # Sembramos el cursor de ingreso de la sesion desde el store durable y se
        # lo comunicamos al cliente: streamea desde resume_from + 1.
        resume_from = self.state.cursor_get(client_id)
        session.last_msg_id = resume_from
        logging.info(
            "Handshake with client %s (presented=%s, resume_from=%d)",
            client_id,
            presented,
            resume_from,
        )
        session.send(
            serialize(
                build_hello_ack_message(
                    client=client_id, sender=self.sender_id, resume_from=resume_from
                )
            )
        )
        return session, client_id

    def _ack(self, client_id, message, session):
        """Confirma al cliente que el mensaje ya fue publicado aguas abajo."""
        session.send(
            serialize(
                build_ack_message(
                    client=client_id,
                    msg_id=message.get("msg_id"),
                    sender=self.sender_id,
                )
            )
        )

    def _forward(self, message):
        """Re-estampa el sender del gateway y reserializa, preservando el msg_id
        del cliente y el resto del mensaje tal cual."""
        message["sender"] = self.sender_id
        return serialize(message)

    def _advance_cursor(self, client_id, session, msg_id):
        """Avanza el cursor de ingreso en memoria tras reenviar downstream y lo
        persiste de forma perezosa (cada PERSIST_EVERY). DESPUES de reenviar: el
        cursor durable nunca debe adelantar a lo realmente reenviado."""
        if msg_id > session.last_msg_id:
            session.last_msg_id = msg_id
        if msg_id % PERSIST_EVERY == 0:
            self.state.cursor_record(client_id, session.last_msg_id)

    def _on_transactions(self, client_id, message, session):
        msg_id = message.get("msg_id")
        # Guard defensivo: un batch ya reenviado (reentrega del cliente) no se
        # vuelve a publicar; igual seria deduplicado aguas abajo.
        if msg_id <= session.last_msg_id:
            return False
        self.router.send_transactions(self._forward(message), msg_id)
        self._advance_cursor(client_id, session, msg_id)
        return False

    def _on_accounts(self, client_id, message, session):
        msg_id = message.get("msg_id")
        if msg_id <= session.last_msg_id:
            return False
        batch = message.get("payload", {}).get("batch", [])
        logging.info(
            "sending accounts batch of %d rows for client %s", len(batch), client_id
        )
        self.router.send_accounts(self._forward(message))
        self._advance_cursor(client_id, session, msg_id)
        return False

    def _on_eof(self, client_id, message, session):
        msg_id = message.get("msg_id")
        # Solo forwardeamos un EOF NUEVO (msg_id > cursor). Un EOF reenviado (un
        # cliente que reanudo directo a la fase de resultados re-manda su EOF para
        # que le señalemos completitud) YA fue propagado: re-forwardearlo dispara
        # re-flushes NO idempotentes aguas abajo (p.ej. q3 historical_filter
        # re-procesa su WAL ya borrado) que corrompen el resultado. Igual lo
        # ackeamos y evaluamos completitud para poder mandar results_done.
        is_new = msg_id is None or msg_id > session.last_msg_id
        if is_new:
            logging.info(
                "Inbound EOF for client %s; forwarding EOF downstream", client_id
            )
            self.router.send_eof(self._forward(message))
            # El EOF es barrera: persistimos el cursor (= msg_id del EOF) ANTES de
            # ackear, asi un cliente que recibio el ack del EOF tiene garantizado
            # que el gateway sabe, de forma durable, que termino de enviar.
            if msg_id is not None:
                session.last_msg_id = msg_id
            self.state.cursor_record(client_id, session.last_msg_id)
        else:
            logging.info(
                "Duplicate EOF for client %s (msg_id=%s already forwarded); "
                "acking without re-forwarding",
                client_id,
                msg_id,
            )
        self._ack(client_id, message, session)
        # Si los EOF de resultado ya estaban todos persistidos (el gateway se
        # cayo y revivio, y el cliente reenvio su EOF para reanudar), no van a
        # volver a llegar por la cola: liberamos `done` aca para cerrar al
        # cliente en vez de dejarlo colgado esperando.
        if self.state.results_complete(client_id, self.expected_results):
            logging.info(
                "Results already complete for client %s; closing on resumed EOF",
                client_id,
            )
            session.done.set()
        return True  # corta el loop de lectura: pasamos a esperar resultados


class ResultConsumer:
    """Consume los resultados que los workers devuelven y los reenvia al cliente
    correcto. Cuenta los EOF de resultado por sesion (de forma DURABLE, via
    GatewayState) y, al alcanzar expected_results, libera (done) al
    thread del cliente para que cierre.

    Tolerancia a fallos: si el resultado no se puede entregar todavia (el cliente
    esta reconectando o aun no reconecto tras un restart del gateway), el mensaje
    se reencola (nack) en vez de descartarse, para no perderlo. El progreso de
    EOFs se persiste ANTES de ackear, asi una caida del gateway no lo pierde.
    """

    def __init__(self, result_mw, registry, expected_results, state, reaper):
        self.result_mw = result_mw
        self.registry = registry
        self.expected_results = expected_results
        self.state = state
        # Declara la caida del cliente cuando un send falla con el gateway vivo.
        self.reaper = reaper

    def run(self):
        self.result_mw.start_consuming(self._dispatch_result)

    def _dispatch_result(self, message, ack, nack):
        try:
            decoded = deserialize(message)
        except Exception:
            logging.exception("Error decoding result message; discarding")
            ack()
            return

        client_id = decoded.get("client")
        is_eof = bool(decoded.get("eof"))

        # El progreso de EOFs se persiste primero y es independiente de que el
        # cliente tenga sesion viva: tras un restart del gateway el resultado
        # puede llegar antes de que el cliente reconecte. record() es idempotente
        # (set por tipo de query), asi que una reentrega no infla el conteo.
        if is_eof:
            count = self.state.result_record(client_id, decoded.get("type"))
            logging.info(
                "Result EOF %d/%d persisted for client %s (%s)",
                count,
                self.expected_results,
                client_id,
                decoded.get("type"),
            )

        session = self.registry.get(client_id)
        if session is None:
            # Sin sesion viva: dos casos distintos.
            # - El gateway aun conoce el UUID (cliente dentro de su ventana de
            #   gracia post-restart, todavia no reconecto): reencolar para
            #   entregarlo cuando vuelva.
            # - El gateway ya NO lo conoce (caido y olvidado, o completado): el
            #   resultado es de un cliente muerto; ackear y descartar para no
            #   reencolar para siempre.
            if self.state.knows_uuid(client_id):
                logging.info(
                    "No session for client %s yet; requeueing result", client_id
                )
                time.sleep(_REQUEUE_BACKOFF)
                nack()
            else:
                logging.info(
                    "Result for unknown/forgotten client %s; discarding", client_id
                )
                ack()
            return

        try:
            session.send(message)
        except Exception:
            # El send fallo con el gateway vivo: el cliente murio. Lo damos por
            # caido (wipe + limpieza) y descartamos el resultado.
            logging.warning(
                "Could not forward result to client %s; declaring crash", client_id
            )
            self.reaper.declare_crashed(client_id)
            ack()
            return

        if is_eof and self.state.results_complete(client_id, self.expected_results):
            session.done.set()

        ack()
