import logging
import queue
import socket
import subprocess
import threading
from dataclasses import dataclass
from typing import List

try:
    from .message import Message
except ImportError:  # ejecucion standalone (test.py / chaos)
    from message import Message

PORT = 54315
IP = "0.0.0.0"

HEALTH_CHECK_INTERVAL = 1.0  # seconds between rounds of checking all peers liveness
PROBE_TIMEOUT = 0.5  # max wait for a pong before considering a peer dead
CONNECT_TIMEOUT = 0.5  # max wait when opening any outgoing connection
COORDINATOR_WAIT = 3.0  # max wait for a "coordinator" after getting an "ok"

# Control signal placed on the queue by `_serve` to ask the controller thread to
# run its own election (Bully: a node that receives an election also starts one).
ELECTION_REQUEST = "e_req"


@dataclass(frozen=True)
class Peer:
    node_id: str
    host: str  # hostname; resolved lazily on each connection (see _send)
    port: int

    @property
    def addr(self) -> tuple[str, int]:
        return (self.host, self.port)


class Node:
    """Detect failed nodes and restarts them"""

    def __init__(
        self,
        node_id: str,
        peers: list[tuple[str, str]],
        peer_containers: dict[str, str],
    ):
        """
        peers:           list of (node_id, hostname), excluding self. El puerto
                         es fijo (PORT) y lo pone el propio Node.
        peer_containers: {node_id: docker_container_name} used to restart peers.
        """
        self.node_id = node_id
        self._peer_containers = peer_containers

        # Keep the hostnames and resolve them lazily on each connection (_send).
        # Resolving here at startup would crash a node that is being restarted
        # while a peer is also down: Docker drops the DNS entry of a stopped
        # container
        self._peers = [Peer(nid, host, PORT) for nid, host in peers]

        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Who we currently believe is the leader (None until the first election).
        self._leader_id: str | None = None

        self._coordinator_event = threading.Event()
        self._stop_event = threading.Event()
        self._queue: queue.Queue = queue.Queue()

        # True while an election is in progress
        self._electing = False

        # Restarts run on a dedicated worker so they never block the control
        # thread: the leader just drops node ids here and keeps coordinating.
        self._restart_queue: queue.Queue = queue.Queue()
        threading.Thread(target=self._restart_worker, daemon=True).start()

    # --- lifecycle ---------------------------------------------------------

    def start(self):
        self._server.bind((IP, PORT))
        listen_backlog = len(self._peers) * 2
        self._server.listen(listen_backlog)
        logging.info("Node %s started, peers: %s", self.node_id, self._peers)

        threading.Thread(target=self._serve, daemon=True).start()
        self._monitor_loop()  # blocks the calling thread

    def stop(self):
        self._stop_event.set()
        self._server.close()

    def _is_leader(self) -> bool:
        return self._leader_id == self.node_id

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
                msg = Message.from_socket(conn)
                if msg is None:
                    continue

                if msg.msg_type == "ping":
                    conn.sendall(Message("pong", self.node_id).to_bytes())

                elif msg.msg_type == "election":
                    # A lower-id node started an election: acknowledge so it backs
                    # off, then ask our controller to run our own.
                    conn.sendall(Message("ok", self.node_id).to_bytes())
                    self._queue.put(ELECTION_REQUEST)

                elif msg.msg_type == "coordinator":
                    if msg.sender != self._leader_id:
                        logging.info("New leader is %s", msg.sender)
                    self._leader_id = msg.sender
                    self._coordinator_event.set()
            except (OSError, ValueError):
                pass
            finally:
                conn.close()

    # --- failure detection + control loop (thread 2) -----------------------

    def _monitor_loop(self):
        # Bootstrap: learn (or become) the leader before steady state.
        self._run_election()

        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=HEALTH_CHECK_INTERVAL)
            except queue.Empty:
                self._handle_health_check_interval()
                continue

            if item == ELECTION_REQUEST:
                self._run_election()

    def _handle_health_check_interval(self):
        """One round of health checks; react based on who is down."""
        failed = self._detect_failures()
        if not failed:
            return

        if self._is_leader():
            # Only the leader revives nodes, using its own detection.
            for node_id in failed:
                self._restart_queue.put(node_id)
        elif self._leader_id is None or self._leader_id in failed:
            # The leader is gone (or unknown): elect a new one.
            self._run_election()

    def _detect_failures(self) -> List[str]:
        failed = []
        for peer in self._peers:
            if not self._healthcheck(peer):
                logging.warning("Detected failed node: %s:%s", peer.host, peer.node_id)
                failed.append(peer.node_id)
        return failed

    def _healthcheck(self, peer: Peer) -> bool:
        reply = _send(peer, Message("ping", self.node_id), expect_reply=True)
        return reply is not None and reply.msg_type == "pong"

    # --- leader election (Bully algorithm) ---------------------------------

    def _run_election(self):
        """
        Contact every peer with a higher id. If any answers "ok" we are not the
        leader and wait for its coordinator announcement; if none answers we win.
        """
        if self._electing:
            return
        self._electing = True
        try:
            self._coordinator_event.clear()
            while not self._stop_event.is_set():
                higher = [p for p in self._peers if int(p.node_id) > int(self.node_id)]
                got_ok = False
                for peer in higher:
                    reply = _send(
                        peer, Message("election", self.node_id), expect_reply=True
                    )
                    if reply is not None and reply.msg_type == "ok":
                        got_ok = True

                if not got_ok:
                    # No higher node alive: we are the new coordinator.
                    self._become_leader()
                    return

                # A higher node took over; wait for it to announce itself. If it
                # dies before doing so, run another round.
                if self._coordinator_event.wait(COORDINATOR_WAIT):
                    return
                logging.warning("No coordinator announced, restarting election")
        finally:
            self._electing = False

    def _become_leader(self):
        self._leader_id = self.node_id
        logging.info("I am the new leader: %s", self.node_id)
        for peer in self._peers:
            _send(peer, Message("coordinator", self.node_id))

    # --- node revival (dedicated worker thread) ----------------------------

    def _restart_worker(self):
        """Drain the restart queue, reviving one node at a time, forever."""
        while not self._stop_event.is_set():
            node_id = self._restart_queue.get()
            self._restart_node(node_id)

    def _restart_node(self, node_id: str):
        container = self._peer_containers.get(node_id, node_id)
        logging.info("Restarting container: %s", container)
        result = subprocess.run(
            ["docker", "start", container], capture_output=True, text=True
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
                return Message.from_socket(sock)
    except (OSError, ValueError):
        return None
    return None
