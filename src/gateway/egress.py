import logging
import time

from src.common.communication.internal import deserialize

# Espera corta antes de reencolar un resultado que todavia no se pudo entregar
# (cliente desconectado). Con prefetch=1 el broker reentrega de inmediato; sin
# esta pausa el consumidor spinnea a full hasta que el cliente reconecte.
_REQUEUE_BACKOFF = 0.5


class ResultConsumer:
    """Consume los resultados que los workers devuelven y los reenvia al cliente
    correcto. Cuenta los EOF de resultado por sesion (de forma DURABLE, via
    GatewayResultProgress) y, al alcanzar expected_results, libera (done) al
    thread del cliente para que cierre.

    Tolerancia a fallos: si el resultado no se puede entregar todavia (el cliente
    esta reconectando o aun no reconecto tras un restart del gateway), el mensaje
    se reencola (nack) en vez de descartarse, para no perderlo. El progreso de
    EOFs se persiste ANTES de ackear, asi una caida del gateway no lo pierde.
    """

    def __init__(self, result_mw, registry, expected_results, progress):
        self.result_mw = result_mw
        self.registry = registry
        self.expected_results = expected_results
        self.progress = progress

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
            count = self.progress.record(client_id, decoded.get("type"))
            logging.info(
                "Result EOF %d/%d persisted for client %s (%s)",
                count,
                self.expected_results,
                client_id,
                decoded.get("type"),
            )

        session = self.registry.get(client_id)
        if session is None:
            # Cliente aun no (re)conectado: reencolar para entregarlo cuando
            # vuelva, en vez de descartar el resultado.
            logging.info(
                "No session for client %s yet; requeueing result", client_id
            )
            time.sleep(_REQUEUE_BACKOFF)
            nack()
            return

        try:
            session.send(message)
        except Exception:
            # El cliente cayo/esta reconectando: no perder el resultado ni
            # completar prematuramente. Reencolar y reintentar al reconectar.
            logging.warning(
                "Could not forward result to client %s (reconnecting?); requeueing",
                client_id,
            )
            time.sleep(_REQUEUE_BACKOFF)
            nack()
            return

        if is_eof and self.progress.is_complete(client_id, self.expected_results):
            session.done.set()

        ack()
