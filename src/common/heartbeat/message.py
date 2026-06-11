from dataclasses import dataclass, asdict
import json


@dataclass
class HeartbeatMessage:
    msg_type: str  # "heartbeat" | "election" | "ok"
    sender: str # name/id from sender
    role: str # master | slave
    timestamp: float # time when the sender created this message
    epoch: int # epoch used for indicate if a new master has been selected

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes):
        return cls(**json.loads(data.decode("utf-8")))
