import json
from dataclasses import asdict, dataclass
from typing import List


@dataclass
class Message:
    msg_type: str  # "ping" | "pong" | "election" | "ok" | "coordinator"
    sender: str  # node id of the sender
    failed_nodes: List[str] | None = None  # down nodes detected (only for "election")

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        return cls(**json.loads(data.decode("utf-8")))
