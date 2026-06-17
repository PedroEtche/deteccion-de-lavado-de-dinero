from typing import Callable
import logging

class EofCoordinator:
    """
    Counts incoming EOFs for a specific client. 
    Once expected_eofs is reached, it triggers the flush callback.
    """

    def __init__(
        self,
        expected_eofs: int,
        on_flush: Callable[[str], None],
        state_manager
    ) -> None:
        if expected_eofs <= 0:
            raise ValueError("expected_eofs must be positive")

        self._expected = expected_eofs
        self._on_flush = on_flush
        self.state_manager = state_manager

        self.eofs_by_client = self.state_manager.load_all()

    def handle_eof(self, client_id: str) -> int:
        """
        Called by the worker when it receives an EOF directly from upstream.
        """
        client_eofs = self.eofs_by_client.get(client_id, 0)
        client_eofs += 1

        self.eofs_by_client[client_id] = client_eofs
        self.state_manager.save_client(client_id, client_eofs)

        if client_eofs >= self._expected:
            self._on_flush(client_id)
            self.eofs_by_client.pop(client_id, None)
            self.state_manager.delete_client(client_id)

        

# import logging
# import threading
# import uuid
# from contextlib import contextmanager
# from typing import Callable, Optional

# from src.common.middleware import MessageMiddlewareExchangeRabbitMQ
# from src.common.communication.internal import (
#     build_eof_message,
#     deserialize,
#     serialize,
# )

# EOF_ROUTING_KEY = "eof"


# class EofCoordinator:
#     """
#     Coordina el flush de un stage con réplicas cuando llegan los EOFs de upstream.

#     Cada réplica del stage rebroadcastea por un fanout intra-stage los EOFs
#     que recibe de upstream. Un thread listener cuenta los broadcasts hasta
#     alcanzar ``expected_eofs`` y dispara ``on_flush(client_id)`` bajo lock.

#     El lock se expone vía :meth:`lock` para que el handler de datos del worker
#     pueda protegerse contra el flush. El flush en sí ocurre bajo el mismo lock,
#     así que cubre el ciclo completo (pop de estado, envío downstream, EOF).

#     El coordinator usa dos instancias del middleware (una por thread): no
#     comparte channel/connection entre el thread principal y el listener.
#     """

#     def __init__(
#         self,
#         mom_host: str,
#         fanout_name: str,
#         expected_eofs: int,
#         on_flush: Callable[[str], None],
#     ) -> None:
#         if expected_eofs <= 0:
#             raise ValueError("expected_eofs must be positive")

#         self._expected = expected_eofs
#         self._on_flush = on_flush
#         self._counts: dict[str, int] = {}
#         self._lock = threading.Lock()
#         self._stop_event = threading.Event()
#         self._listener_thread: Optional[threading.Thread] = None

#         self._publisher = MessageMiddlewareExchangeRabbitMQ(
#             host=mom_host,
#             exchange_name=fanout_name,
#             routing_keys=[EOF_ROUTING_KEY],
#         )
#         self._consumer = MessageMiddlewareExchangeRabbitMQ(
#             host=mom_host,
#             exchange_name=fanout_name,
#             routing_keys=[EOF_ROUTING_KEY],
#         )

#     @contextmanager
#     def lock(self):
#         """Context manager que protege el estado del worker contra el flush.

#         Lo usa tanto el handler de datos del worker (en el main thread) como
#         el callback de flush interno (en el listener thread).
#         """
#         with self._lock:
#             yield

#     def broadcast(self, client_id: str) -> None:
#         """Rebroadcastea un EOF al fanout intra-stage.

#         Lo llama el main thread cuando recibe un EOF desde upstream.
#         """
#         msg = build_eof_message(client=client_id, msg_id=str(uuid.uuid4()))
#         self._publisher.send(serialize(msg))

#     def start(self) -> None:
#         """Arranca el listener thread. No es daemon: graceful stop espera al join."""
#         if self._listener_thread is not None:
#             raise RuntimeError("EofCoordinator already started")
#         self._listener_thread = threading.Thread(
#             target=self._consume,
#             name="eof-coordinator-listener",
#             daemon=False,
#         )
#         self._listener_thread.start()

#     def stop(self, timeout: Optional[float] = None) -> None:
#         """Detiene el listener gracefully.

#         Si hay un flush en curso, espera a que termine (se ejecuta dentro del
#         callback de pika; al volver, el ``stop_consuming`` agendado corre
#         recién después). Cross-thread safe vía ``add_callback_threadsafe``.
#         """
#         if self._stop_event.is_set():
#             return
#         self._stop_event.set()

#         if self._listener_thread is not None and self._listener_thread.is_alive():
#             try:
#                 self._consumer.connection.add_callback_threadsafe(
#                     self._consumer.channel.stop_consuming
#                 )
#             except Exception:
#                 logging.exception("failed to schedule stop_consuming on eof consumer")
#             self._listener_thread.join(timeout=timeout)

#     def close(self) -> None:
#         """Cierra las conexiones del coordinator. Idempotente."""
#         for mw in (self._publisher, self._consumer):
#             try:
#                 mw.close()
#             except Exception:
#                 logging.exception("error closing eof middleware")

#     def _consume(self) -> None:
#         try:
#             self._consumer.start_consuming(self._on_broadcast)
#         except Exception:
#             if not self._stop_event.is_set():
#                 logging.exception("eof listener crashed")

#     def _on_broadcast(self, body, ack, _nack):
#         msg = deserialize(body)
#         client_id = msg["client"]
#         self._counts[client_id] = self._counts.get(client_id, 0) + 1
#         if self._counts[client_id] >= self._expected:
#             with self._lock:
#                 self._on_flush(client_id)
#             self._counts.pop(client_id, None)
#         ack()
