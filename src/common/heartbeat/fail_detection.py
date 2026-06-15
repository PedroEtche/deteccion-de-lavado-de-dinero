import logging
import queue
import socket
import subprocess
import threading
from dataclasses import dataclass
from typing import List

from message import Message

PORT = 54315
IP = "0.0.0.0"

PROBE_INTERVAL = 1.0  # seconds between rounds of probing all peers
PROBE_TIMEOUT = 0.5  # max wait for a pong before considering a peer dead
CONNECT_TIMEOUT = 0.5  # max wait when opening any outgoing connection
COORDINATOR_WAIT = 3.0  # max wait for a "coordinator" after getting an "ok"

# Control signal placed on the queue by `_serve` to ask the controller thread to
# run its own election (Bully: a node that receives an election also starts one).
ELECTION_REQUEST = "election_request"


@dataclass(frozen=True)
class Peer:
    node_id: str
    ip: str
    port: int

    @property
    def addr(self) -> tuple[str, int]:
        return (self.ip, self.port)


class Node:
    def __init__(
        self,
        node_id: str,
        peers: list[tuple[str, str, int]],
        container_name: str,
        peer_containers: dict[str, str],
    ):
        """
        peers:           list of (node_id, hostname, port), excluding self.
        peer_containers: {node_id: docker_container_name} used to restart peers.
        """
        self.node_id = node_id
        self._container_name = container_name
        self._peer_containers = peer_containers

        # Resolve hostnames once at startup: when a node dies Docker may drop its
        # DNS entry, and we still want to be able to talk to the survivors.
        self._peers = [
            Peer(nid, socket.gethostbyname(host), port) for nid, host, port in peers
        ]

        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self._coordinator_event = threading.Event()
        self._stop_event = threading.Event()
        self._queue: queue.Queue = queue.Queue()

        # True while an election is in progress (coalesces concurrent triggers).
        self._electing = False

    # --- lifecycle ---------------------------------------------------------

    def start(self):
        self._server.bind((IP, PORT))
        self._server.listen(16)
        logging.info("Node %s started, peers: %s", self.node_id, self._peers)

        threading.Thread(target=self._serve, daemon=True).start()
        self._monitor_loop()  # blocks the calling thread

    def stop(self):
        self._stop_event.set()
        self._server.close()

    # --- incoming messages (TCP server, thread 1) --------------------------

    def _serve(self):
        """Accept one connection at a time; each connection carries one message.

        Everything is handled inline and fast so the node keeps answering pings
        even while the controller thread is blocked running an election.
        """
        while not self._stop_event.is_set():
            try:
                conn, _ = self._server.accept()
            except OSError:
                break

            try:
                data = conn.recv(1024)
                if not data:
                    continue
                try:
                    msg = Message.from_bytes(data)
                except ValueError:
                    continue

                if msg.msg_type == "ping":
                    conn.sendall(Message("pong", self.node_id).to_bytes())

                elif msg.msg_type == "election":
                    # A lower-id node started an election: acknowledge so it backs
                    # off, then ask our controller to run our own.
                    # Pass along the failed nodes so the eventual leader can
                    # restart them.
                    conn.sendall(Message("ok", self.node_id).to_bytes())
                    self._queue.put((ELECTION_REQUEST, msg.failed_nodes or []))

                elif msg.msg_type == "coordinator":
                    self._coordinator_event.set()
                    logging.info("New leader is %s", msg.sender)
            except OSError:
                pass
            finally:
                conn.close()

    # --- failure detection + control loop (thread 2) -----------------------

    def _monitor_loop(self):
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=PROBE_INTERVAL)
            except queue.Empty:
                failed_nodes = self._detect_failures()
                if failed_nodes:
                    self._run_election(failed_nodes)
                continue

            if item[0] == ELECTION_REQUEST:
                self._run_election(item[1])

    def _detect_failures(self) -> List[str]:
        failed = []
        for peer in self._peers:
            if not self._healthcheck(peer):
                logging.warning("Detected failed node: %s", peer.node_id)
                failed.append(peer.node_id)
        return failed

    def _healthcheck(self, peer: Peer) -> bool:
        reply = _send(peer, Message("ping", self.node_id), expect_reply=True)
        return reply is not None and reply.msg_type == "pong"

    # --- leader election (Bully algorithm) ---------------------------------

    def _run_election(self, failed_nodes: List[str]):
        """
        Contact every peer with a higher id. If any answers "ok" we are not the
        leader and wait for its coordinator announcement; if none answers we win.
        Re-runs on every detected failure (we don't care about a stable leader).
        """
        if self._electing:
            return
        self._electing = True
        try:
            self._coordinator_event.clear()
            while not self._stop_event.is_set():
                higher = [p for p in self._peers if p.node_id > self.node_id]
                got_ok = False
                for peer in higher:
                    msg = Message("election", self.node_id, failed_nodes)
                    reply = _send(peer, msg, expect_reply=True)
                    if reply is not None and reply.msg_type == "ok":
                        got_ok = True

                if not got_ok:
                    # No higher node alive: we are the new coordinator.
                    self._become_leader(failed_nodes)
                    return

                # A higher node took over; wait for it to announce itself. If it
                # dies before doing so, run another round.
                if self._coordinator_event.wait(COORDINATOR_WAIT):
                    return
                logging.warning("No coordinator announced, restarting election")
        finally:
            self._electing = False

    def _become_leader(self, failed_nodes: List[str]):
        logging.info("I am the new leader: %s", self.node_id)
        for peer in self._peers:
            _send(peer, Message("coordinator", self.node_id))
        for node_id in failed_nodes:
            self._restart_node(node_id)

    def _restart_node(self, node_id: str):
        container = self._peer_containers.get(node_id, node_id)
        logging.info("Restarting container: %s", container)
        result = subprocess.run(
            ["docker", "restart", container], capture_output=True, text=True
        )
        if result.returncode == 0:
            logging.info("Successfully restarted %s", container)
        else:
            logging.error("Failed to restart %s: %s", container, result.stderr.strip())


# --- networking helper -------------------------------------------------


def _send(peer: Peer, msg: Message, expect_reply: bool = False) -> Message | None:
    """Open a TCP connection, send one message, optionally read one reply."""
    try:
        with socket.create_connection(peer.addr, timeout=CONNECT_TIMEOUT) as sock:
            sock.settimeout(PROBE_TIMEOUT)
            sock.sendall(msg.to_bytes())
            if expect_reply:
                data = sock.recv(1024)
                return Message.from_bytes(data) if data else None
    except (OSError, ValueError):
        return None
    return None
