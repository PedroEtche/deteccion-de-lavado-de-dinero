import json
import socket
import struct
from dataclasses import asdict, dataclass


@dataclass
class Message:
    msg_type: str  # "ping" | "pong" | "election" | "ok" | "coordinator"
    sender: str  # node id of the sender

    def to_bytes(self):
        payload = json.dumps(asdict(self)).encode("utf-8")
        return struct.pack(">I", len(payload)) + payload

    @classmethod
    def from_socket(cls, sock: socket.socket):
        """Read one length-prefixed message, or None if the peer closed early."""
        header = _recvall(sock, 4)
        if header is None:
            return None
        (length,) = struct.unpack(">I", header)
        payload = _recvall(sock, length)
        if payload is None:
            return None
        return cls(**json.loads(payload.decode("utf-8")))


def _recvall(sock: socket.socket, n: int):
    """Read exactly n bytes from sock, looping over recv to defeat short reads.

    Returns None if the peer closes before n bytes arrive.
    """
    chunks = bytearray()
    while len(chunks) < n:
        chunk = sock.recv(n - len(chunks))
        if not chunk:
            return None
        chunks.extend(chunk)
    return bytes(chunks)
