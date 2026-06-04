import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from src.common.eof.coordinator import EOF_ROUTING_KEY, EofCoordinator
from common.communication.internal import deserialize


class _FakeExchange:
    """Doble de MessageMiddlewareExchangeRabbitMQ para tests.

    Mantiene un buffer de mensajes publicados. Si dos FakeExchange comparten
    el mismo `network`, los mensajes publicados en uno son visibles para los
    consumers del otro (simula el fanout).
    """

    _instances: dict[str, list["_FakeExchange"]] = {}

    def __init__(self, host, exchange_name, routing_keys=None):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys or []
        self.connection = MagicMock()
        self.channel = MagicMock()
        self._callback = None
        self._consuming = False
        self._closed = False
        self._pending: list[bytes] = []
        self._cond = threading.Condition()
        self.published: list[bytes] = []

        _FakeExchange._instances.setdefault(exchange_name, []).append(self)
        # connection.add_callback_threadsafe simula la cola IO de pika
        self.connection.add_callback_threadsafe.side_effect = self._schedule
        # channel.stop_consuming corta el loop del consumer
        self.channel.stop_consuming.side_effect = self.stop_consuming

    def _schedule(self, callback):
        # Ejecuta el callback en el thread del consumer (signal-style).
        with self._cond:
            self._pending.append(("__call__", callback))
            self._cond.notify_all()

    def send(self, message, routing_key=None):
        self.published.append(message)
        # Broadcast a todos los consumers bindeados al mismo exchange.
        for inst in _FakeExchange._instances.get(self.exchange_name, []):
            if inst is self:
                continue
            with inst._cond:
                inst._pending.append(("message", message))
                inst._cond.notify_all()

    def start_consuming(self, on_message_callback):
        self._callback = on_message_callback
        self._consuming = True
        while self._consuming:
            with self._cond:
                while not self._pending and self._consuming:
                    self._cond.wait(timeout=0.5)
                if not self._consuming:
                    return
                kind, payload = self._pending.pop(0)
            if kind == "__call__":
                payload()
            elif kind == "message":
                self._callback(payload, lambda: None, lambda: None)

    def stop_consuming(self):
        with self._cond:
            self._consuming = False
            self._cond.notify_all()

    def close(self):
        self._closed = True

    @classmethod
    def reset(cls):
        cls._instances.clear()


def _patch_middleware():
    return patch(
        "src.common.eof.coordinator.MessageMiddlewareExchangeRabbitMQ",
        _FakeExchange,
    )


class EofCoordinatorTest(unittest.TestCase):
    def setUp(self):
        _FakeExchange.reset()
        self.patcher = _patch_middleware()
        self.patcher.start()
        self.flushed: list[str] = []
        self.flush_event = threading.Event()
        self.flush_delay = 0.0

        def on_flush(client_id):
            if self.flush_delay:
                time.sleep(self.flush_delay)
            self.flushed.append(client_id)
            self.flush_event.set()

        self.on_flush = on_flush

    def tearDown(self):
        self.patcher.stop()

    def _make(self, expected=2):
        return EofCoordinator(
            mom_host="ignored",
            fanout_name="stage_eof",
            expected_eofs=expected,
            on_flush=self.on_flush,
        )

    def test_rejects_non_positive_expected(self):
        with self.assertRaises(ValueError):
            EofCoordinator("h", "f", 0, self.on_flush)
        with self.assertRaises(ValueError):
            EofCoordinator("h", "f", -1, self.on_flush)

    def test_broadcast_publishes_eof_with_fresh_msg_id(self):
        coord = self._make(expected=1)
        coord.broadcast("client-A")
        coord.broadcast("client-A")

        published = coord._publisher.published
        self.assertEqual(len(published), 2)
        msgs = [deserialize(b) for b in published]
        self.assertEqual(msgs[0]["type"], "eof")
        self.assertEqual(msgs[0]["client"], "client-A")
        self.assertNotEqual(msgs[0]["msg_id"], msgs[1]["msg_id"])
        coord.close()

    def test_flush_triggers_after_expected_broadcasts(self):
        coord = self._make(expected=3)
        coord.start()
        try:
            coord.broadcast("c1")
            coord.broadcast("c1")
            # con 2 broadcasts todavia no debe flushear
            self.assertFalse(self.flush_event.wait(timeout=0.2))
            coord.broadcast("c1")
            self.assertTrue(self.flush_event.wait(timeout=1.0))
            self.assertEqual(self.flushed, ["c1"])
        finally:
            coord.stop(timeout=2)
            coord.close()

    def test_flush_per_client_independent(self):
        coord = self._make(expected=2)
        coord.start()
        try:
            coord.broadcast("c1")
            coord.broadcast("c2")
            coord.broadcast("c1")
            # esperar a que c1 flushee
            for _ in range(50):
                if "c1" in self.flushed:
                    break
                time.sleep(0.02)
            self.assertIn("c1", self.flushed)
            self.assertNotIn("c2", self.flushed)
            coord.broadcast("c2")
            for _ in range(50):
                if "c2" in self.flushed:
                    break
                time.sleep(0.02)
            self.assertIn("c2", self.flushed)
        finally:
            coord.stop(timeout=2)
            coord.close()

    def test_lock_serializes_data_handler_against_flush(self):
        """El handler de datos no debe correr en paralelo con on_flush."""
        coord = self._make(expected=1)
        self.flush_delay = 0.15
        observed_overlap = threading.Event()
        data_running = threading.Event()
        flush_running = threading.Event()

        # Reemplazamos on_flush por uno que avise mientras corre.
        def slow_flush(client_id):
            flush_running.set()
            time.sleep(self.flush_delay)
            if data_running.is_set():
                observed_overlap.set()
            self.flushed.append(client_id)
            self.flush_event.set()

        coord._on_flush = slow_flush
        coord.start()
        try:
            coord.broadcast("c1")
            # Esperar a que el flush realmente arranque y tome el lock.
            self.assertTrue(flush_running.wait(timeout=1.0))

            # Intentar tomar el lock del handler de datos en paralelo.
            t = threading.Thread(
                target=self._try_take_lock_during_flush,
                args=(coord, data_running),
            )
            t.start()
            t.join(timeout=2)
            self.assertTrue(self.flush_event.wait(timeout=1.0))
            self.assertFalse(observed_overlap.is_set())
        finally:
            coord.stop(timeout=2)
            coord.close()

    @staticmethod
    def _try_take_lock_during_flush(coord, data_running_flag):
        with coord.lock():
            data_running_flag.set()

    def test_stop_is_idempotent(self):
        coord = self._make(expected=1)
        coord.start()
        coord.stop(timeout=1)
        coord.stop(timeout=1)  # no debe explotar
        coord.close()

    def test_start_twice_raises(self):
        coord = self._make(expected=1)
        coord.start()
        try:
            with self.assertRaises(RuntimeError):
                coord.start()
        finally:
            coord.stop(timeout=1)
            coord.close()

    def test_close_does_not_raise_after_stop(self):
        coord = self._make(expected=1)
        coord.start()
        coord.stop(timeout=1)
        coord.close()


if __name__ == "__main__":
    unittest.main()
