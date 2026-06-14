import logging
import queue
import socket
import threading
import time

from message import HeartbeatMessage

PORT = 54315
IP = "0.0.0.0"  # TODO: Habria que capaz ver que IP otorga compose o si eso importa realmente. Usando esta IP parece andar
HEARTBEAT_INTERVAL = 1.0


class Heartbeat:
    """
    Send a periodic heartbeat message to a list of target addresses.
    If a message hasn't been received from a target for a certain amount of time, we can consider it failed.
    In a failure scenario a leader election will be triggered, and the new leader will be responsible for
    restarting the failed node (docker-in-docker).
    """

    def __init__(self, sender_id: str, targets: list):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sender_id = sender_id
        # Resolve hostnames to IPs once at startup so that DNS removal when a
        # node dies does not crash the sender thread
        self._targets = [(socket.gethostbyname(host), port) for host, port in targets]
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self.heartbeats_received = {}  # {sender_id: last_local_recv_timestamp}

    def start(self):
        self._socket.bind((IP, PORT))
        logging.info("Heartbeat started for %s", self._sender_id)
        threading.Thread(target=self._listen, daemon=True).start()
        self._run()  # blocks the calling thread

    def _listen(self):
        """Passively receive UDP messages and forward them to the controller via queue."""
        while True:
            data, addr = self._socket.recvfrom(1024)
            msg = HeartbeatMessage.from_bytes(data)
            self._queue.put((msg, addr))

    def _run(self):
        """Controller loop: send heartbeats on interval, process incoming messages."""
        while not self._stop_event.is_set():
            try:
                msg, addr = self._queue.get(timeout=HEARTBEAT_INTERVAL)
            except queue.Empty:
                msg = HeartbeatMessage(msg_type="heartbeat", sender=self._sender_id)
                for target in self._targets:
                    self._socket.sendto(msg.to_bytes(), target)

                self._check_failures()
                continue
            self._handle_message(msg, addr)

    def _handle_message(self, msg: HeartbeatMessage, addr):
        logging.info("received message: %s", msg)
        self.heartbeats_received[msg.sender] = time.time()
        if msg.msg_type == "heartbeat":
            self._check_failures()
        elif msg.msg_type == "election":
            if self._sender_id > msg.sender:
                response = HeartbeatMessage(msg_type="ok", sender=self._sender_id)
                self._socket.sendto(response.to_bytes(), addr)
        elif msg.msg_type == "ok":
            pass  # someone with higher ID exists; cancel our own election
        elif msg.msg_type == "coordinator":
            pass  # record the new leader

    def _check_failures(self):
        now = time.time()
        failed_nodes = []
        for sender, last_timestamp in self.heartbeats_received.items():
            if now - last_timestamp > 3 * HEARTBEAT_INTERVAL:
                failed_nodes.append(sender)
        if failed_nodes:
            logging.warning("Detected failed nodes: %s", failed_nodes)
            self._start_leader_election(failed_nodes)

    def _start_leader_election(self, failed_nodes):
        """Bully algorithm: send election message to nodes with higher IDs."""
        msg = HeartbeatMessage(
            msg_type="election", sender=self._sender_id, failed_nodes=failed_nodes
        )
        for target in self._targets:
            target_id = target[0]
            if target_id in failed_nodes or target_id < self._sender_id:
                continue
            self._socket.sendto(msg.to_bytes(), target)

    def stop(self):
        self._stop_event.set()
