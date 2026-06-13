import logging
import select
import socket
import threading
import time

from message import HeartbeatMessage

PORT = 54315
IP = "127.0.0.1"
HEARTBEAT_INTERVAL = 1.0


class Heartbeat:
    """
    Send a periodic heartbeat message to a list of target addresses.
    If a message hasnt been received from a target fom a certain amount of time, we can consider it failed.
    In a failure scenario a leader election will be triggered, and the new leader will be the responsable of start over the failed node (docker-in-docker).
    """

    def __init__(self, sender_id: str, targets: list):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sender_id = sender_id
        self._targets = targets  # list of (host, port)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = None
        self.heartbeats_received = {}  # {sender_id: last_timestamp}

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        logging.info("Heartbeat started for %s", self._sender_id)
        self._thread.start()
        self._recv()

    def _run(self):
        while not self._stop_event.wait(HEARTBEAT_INTERVAL):
            msg = HeartbeatMessage(
                msg_type="heartbeat",
                sender=self._sender_id,
                timestamp=time.time(),
            )
            for target in self._targets:
                data = msg.to_bytes()
                self._socket.sendto(data, target)

    def _recv(self):
        self._socket.bind((IP, PORT))
        while True:
            data, addr = self._socket.recvfrom(1024)
            msg = HeartbeatMessage.from_bytes(data)
            logging.info("received message: %s", msg)
            self.heartbeats_received[msg.sender] = msg.timestamp
            self._check_failures()

    def _check_failures(self):
        now = time.time()
        failed_nodes = []
        with self._lock:
            for sender, last_timestamp in self.heartbeats_received.items():
                if now - last_timestamp > 3 * HEARTBEAT_INTERVAL:
                    failed_nodes.append(sender)
        if failed_nodes:
            logging.warning("Detected failed nodes: %s", failed_nodes)
            self._start_leader_election(failed_nodes)

    def _start_leader_election(self, failed_nodes):
        """Bully algorithm: the node with the highest ID becomes the leader."""
        pass

    def stop(self):
        self._stop_event.set()
