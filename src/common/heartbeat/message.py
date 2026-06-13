import json
from dataclasses import asdict, dataclass


@dataclass
class HeartbeatMessage:
    msg_type: str  # "heartbeat" | "election" | "ok"
    sender: str  # name/id from sender
    timestamp: float  # time when the sender created this message

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes):
        return cls(**json.loads(data.decode("utf-8")))
