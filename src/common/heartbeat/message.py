import json
from typing import List
from dataclasses import asdict, dataclass


@dataclass
class HeartbeatMessage:
    msg_type: str  # "heartbeat" | "election" | "ok" | "coordinator"
    sender: str  # name/id from sender
    failed_nodes: List[str] | None = None  # Down nodes detected

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes):
        return cls(**json.loads(data.decode("utf-8")))
