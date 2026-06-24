import logging
import socket

from src.common.communication.tcp import TCPSocket
from src.common.communication.internal import (
    build_ack_message,
    build_hello_ack_message,
    build_results_done_message,
    deserialize,
    serialize,
)

# Cada cuantos mensajes de datos se persiste el cursor de ingreso. Es solo una
# palanca de performance: persistir de menos solo provoca re-envio (deduplicado
# aguas abajo) tras una caida, nunca perdida. El EOF persiste siempre.
PERSIST_EVERY = 500


class ClientHandler:
    """Maneja una conexion de cliente: lee mensajes, los despacha al pipeline y
    espera a que vuelvan todos los resultados antes de cerrar la conexion.

    Entrega de datos (streaming): el cliente blastea sus batches con un msg_id
    monotonico SIN ACK por batch. El gateway reenvia cada uno downstream
    (reutilizando el msg_id del cliente) y lleva un cursor durable del ultimo
    msg_id reenviado. Al reconectar, le responde al cliente desde donde retomar
    (ver IngressCursorStore). El EOF si es stop-and-wait: barrera + flush.
    """

    def __init__(
        self,
        client_socket,
        registry,
        router,
        sender_id,
        shutdown_event,
        progress,
        expected_results,
        cursor,
    ):
        self.tcp = TCPSocket(client_socket)
        self.registry = registry
        self.router = router
        self.sender_id = sender_id
        self._shutdown = shutdown_event
        # Progreso durable de EOFs de resultado: permite cerrar a un cliente que,
        # tras un restart del gateway, reconecta y reenvia su EOF cuando los EOFs
        # de resultado ya estaban todos persistidos.
        self.progress = progress
        self.expected_results = expected_results
        # Cursor durable de ingreso (uuid -> ultimo msg_id reenviado downstream).
        self.cursor = cursor
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
                    logging.info("The client connection was closed")
                    break

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
        # Sembramos el cursor de ingreso de la sesion desde el store durable y se
        # lo comunicamos al cliente: streamea desde resume_from + 1.
        resume_from = self.cursor.get(client_id)
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
            self.cursor.record(client_id, session.last_msg_id)

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
        logging.info("Inbound EOF for client %s; forwarding EOF downstream", client_id)
        msg_id = message.get("msg_id")
        self.router.send_eof(self._forward(message))
        # El EOF es barrera: persistimos el cursor (= msg_id del EOF) ANTES de
        # ackear, asi un cliente que recibio el ack del EOF tiene garantizado que
        # el gateway sabe, de forma durable, que termino de enviar.
        if msg_id is not None and msg_id > session.last_msg_id:
            session.last_msg_id = msg_id
        self.cursor.record(client_id, session.last_msg_id)
        self._ack(client_id, message, session)
        # Si los EOF de resultado ya estaban todos persistidos (el gateway se
        # cayo y revivio, y el cliente reenvio su EOF para reanudar), no van a
        # volver a llegar por la cola: liberamos `done` aca para cerrar al
        # cliente en vez de dejarlo colgado esperando.
        if self.progress.is_complete(client_id, self.expected_results):
            logging.info(
                "Results already complete for client %s; closing on resumed EOF",
                client_id,
            )
            session.done.set()
        return True  # corta el loop de lectura: pasamos a esperar resultados
