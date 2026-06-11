import socket
import threading
import time
import logging

from message import HeartbeatMessage

PORT = 54315
IP = ""
HEARTBEAT_INTERVAL = 1.0


class Heartbeat:
    def __init__(self, sender_id: str, role: str, targets: list):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sender_id = sender_id
        self._role = role
        self._targets = targets  # list of (host, port)
        self._epoch = 0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("Heartbeat started for %s (%s)", self._sender_id, self._role)

    def _run(self):
        while not self._stop_event.wait(HEARTBEAT_INTERVAL):
            with self._lock:
                epoch = self._epoch
            msg = HeartbeatMessage(
                msg_type="heartbeat",
                sender=self._sender_id,
                role=self._role,
                timestamp=time.time(),
                epoch=epoch,
            )
            for target in self._targets:
                data = msg.to_bytes()
                self._socket.sendto(data, target)

    def recv(self, on_message=None):
        self._socket.bind((IP, PORT))
        while True:
            data, addr = self._socket.recvfrom(1024)
            msg = HeartbeatMessage.from_bytes(data)
            if on_message:
                on_message(msg, addr)
            else:
                logging.info("received message: %s", msg)

    def set_epoch(self, epoch: int):
        with self._lock:
            self._epoch = epoch

    def stop(self):
        self._stop_event.set()
