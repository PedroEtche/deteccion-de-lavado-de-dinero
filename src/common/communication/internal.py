import json
from datetime import datetime
from json import JSONEncoder
from dataclasses import dataclass, asdict

_TIMESTAMP_FORMATS = ("%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S")

# Metadata about registered message types and their validation rules
MESSAGE_TYPES = {}


def register_message_type(
    name, payload_required=False, required_fields=None, payload_validator=None
):
    """Registers a new message type with the given name and schema."""
    if required_fields is None:
        required_fields = []

    MESSAGE_TYPES[name] = {
        "payload_required": payload_required,
        "required_fields": list(required_fields),
        "payload_validator": payload_validator,
    }


def _validate_batch_payload(payload):
    if not isinstance(payload, dict):
        raise MessageValidationError("payload must be a dictionary")

    if "batch_size" not in payload:
        raise MessageValidationError("payload is missing 'batch_size'")

    if "batch" not in payload:
        raise MessageValidationError("payload is missing 'batch'")

    if not isinstance(payload["batch"], list):
        raise MessageValidationError("payload['batch'] must be a list")

    if not isinstance(payload["batch_size"], int):
        raise MessageValidationError("payload['batch_size'] must be an integer")

    if payload["batch_size"] != len(payload["batch"]):
        raise MessageValidationError(
            "payload['batch_size'] must match the length of payload['batch']"
        )


def _validate_payload(payload):
    if not isinstance(payload, int):
        raise MessageValidationError("payload must be an integer")


# Supported message types
register_message_type(
    "raw_transactions",
    payload_required=True,
    required_fields=["payload"],
    payload_validator=_validate_batch_payload,
)
register_message_type(
    "raw_accounts",
    payload_required=True,
    required_fields=["payload"],
    payload_validator=_validate_batch_payload,
)
register_message_type("eof")
register_message_type(
    "q1_result",
    payload_required=True,
    required_fields=["payload"],
    payload_validator=_validate_batch_payload,
)
register_message_type(
    "q2_result",
    payload_required=True,
    required_fields=["payload"],
    payload_validator=_validate_batch_payload,
)
register_message_type(
    "q3_result",
    payload_required=True,
    required_fields=["payload"],
    payload_validator=_validate_batch_payload,
)
register_message_type(
    "q4_result",
    payload_required=True,
    required_fields=["payload"],
    payload_validator=_validate_batch_payload,
)
register_message_type(
    "q5_result",
    payload_required=True,
    required_fields=["payload"],
    payload_validator=_validate_payload,
)


@dataclass
class Payload:
    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict):
            raise MessageDecodeError(f"{cls.__name__}.from_dict expects a dict")

        return cls(**data)


@dataclass
class AccountRow(Payload):
    bank_name: str | None = None
    bank_id: str | None = None
    account_number: str | None = None
    entity_id: str | None = None
    entity_name: str | None = None


@dataclass
class TransactionRow(Payload):
    timestamp: str | None = None
    from_bank: str | None = None
    from_account: str | None = None
    to_bank: str | None = None
    to_account: str | None = None
    amount_received: float | None = None
    receiving_currency: str | None = None
    amount_paid: float | None = None
    payment_currency: str | None = None
    payment_format: str | None = None

    def __post_init__(self):
        """Used for casting str values to float. The objective is cloning pandas results"""
        if self.amount_received is not None:
            self.amount_received = float(self.amount_received)
        if self.amount_paid is not None:
            self.amount_paid = float(self.amount_paid)

    @property
    def date(self) -> datetime | None:
        if self.timestamp is None:
            return None
        for fmt in _TIMESTAMP_FORMATS:
            try:
                return datetime.strptime(self.timestamp, fmt)
            except (ValueError, TypeError):
                continue
        return None


class _MessageEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Payload):
            return o.to_dict()
        return super().default(o)


class ProtocolError(ValueError):
    pass


class MessageValidationError(ProtocolError):
    pass


class MessageDecodeError(ProtocolError):
    pass


def _validate_message(message):
    if not isinstance(message, dict):
        raise MessageValidationError("message must be a dictionary")

    # INFO: Dejo esto comentador porque agregue nuevos tipos de mensaje (respuestas de queries) que no tiene un field de client y/o msg_id
    # for field_name in ("type", "client", "msg_id"):
    #     if field_name not in message:
    #         raise MessageValidationError(f"message is missing '{field_name}'")

    if not isinstance(message["type"], str) or not message["type"].strip():
        raise MessageValidationError("message['type'] must be a non-empty string")

    schema = MESSAGE_TYPES.get(message["type"])
    if schema is None:
        return

    for field_name in schema["required_fields"]:
        if field_name not in message:
            raise MessageValidationError(
                f"message type '{message['type']}' is missing '{field_name}'"
            )

    if schema["payload_required"] and "payload" not in message:
        raise MessageValidationError(
            f"message type '{message['type']}' requires a payload"
        )

    if "payload" in message and schema["payload_validator"] is not None:
        schema["payload_validator"](message["payload"])


def build_message(message_type, client=None, msg_id=None, payload=None):
    message = {"type": message_type}

    if client is not None:
        message["client"] = client
    if msg_id is not None:
        message["msg_id"] = msg_id
    if payload is not None:
        message["payload"] = payload

    _validate_message(message)
    return message


def build_batch_message(message_type, batch, client=None, msg_id=None):
    payload = {"batch_size": len(batch), "batch": batch}

    return build_message(message_type, client=client, msg_id=msg_id, payload=payload)


def build_raw_transactions_message(*, client, msg_id, batch):
    """Wrapper for building raw transactions message"""
    if not isinstance(batch, list):
        raise MessageValidationError("message must be a list")
    if not all(isinstance(row, TransactionRow) for row in batch):
        raise MessageValidationError("message must be a list of TransactionRow objects")
    return build_batch_message(
        "raw_transactions",
        client=client,
        msg_id=msg_id,
        batch=batch,
    )


def build_raw_accounts_message(*, client, msg_id, batch):
    """Wrapper for building raw accounts message"""
    if not isinstance(batch, list):
        raise MessageValidationError("message must be a list")
    if not all(isinstance(row, AccountRow) for row in batch):
        raise MessageValidationError("message must be a list of AccountRow objects")

    return build_batch_message(
        "raw_accounts",
        client=client,
        msg_id=msg_id,
        batch=batch,
    )


def build_eof_message(*, client, msg_id):
    """Wrapper for building EOF message"""
    return build_message("eof", client=client, msg_id=msg_id)


def build_q1_result(*, batch, eof, client):
    """Wrapper for building q1 result message"""
    if not isinstance(batch, list):
        raise MessageValidationError("message must be a list")
    if not all(isinstance(row, TransactionRow) for row in batch):
        raise MessageValidationError("message must be a list of TransactionRow objects")

    msg = build_batch_message("q1_result", batch=batch, client=client)
    msg["eof"] = eof
    return msg


def serialize(message):
    _validate_message(message)
    return json.dumps(
        message, ensure_ascii=False, separators=(",", ":"), cls=_MessageEncoder
    ).encode("utf-8")


def deserialize(message):
    if not isinstance(message, (bytes, bytearray)):
        raise MessageDecodeError("message must be bytes-like")

    try:
        decoded = json.loads(bytes(message).decode("utf-8"))
    except UnicodeDecodeError as e:
        raise MessageDecodeError("message is not valid UTF-8") from e
    except json.JSONDecodeError as e:
        raise MessageDecodeError("message is not valid JSON") from e

    _validate_message(decoded)

    if "payload" not in decoded:
        return decoded

    payload = decoded["payload"]
    batch = payload.get("batch")

    # Convert batch dictionaries back to objects based on the message type.
    msg_type = decoded.get("type")
    if msg_type in ("raw_transactions", "q1_result"):
        decoded["payload"]["batch"] = [
            TransactionRow.from_dict(row) if isinstance(row, dict) else row
            for row in batch
        ]
    elif msg_type == "raw_accounts":
        decoded["payload"]["batch"] = [
            AccountRow.from_dict(row) if isinstance(row, dict) else row for row in batch
        ]

    return decoded


"""
Add new message/usage guide:
1. register_message_type("your_type", payload_required=True, required_fields=[...], payload_validator=...)
2. build_message("your_type", client=..., msg_id=..., payload=...)
3. call serialize()/deserialize() as usual


Base messages examples:

message = {
    type: "raw_transactions",
    client: uuid,
    msg_id: uuid,
    payload: {
        batch_size: N,
        batch: [row0, row1, ..., rowN]
    },
}

Example raw transaction row
row: {
    Timestamp: 2022/09/02 06:00,
    From Bank: 20,
    Account: 802EABEB0,
    To Bank: 220270,
    Account.1: 80E25DFF0,
    Amount Received: 9661.410000,
    Receiving Currency: USD,
    Amount Paid: 9661.410000,
    Payment Currency: USD,
    Payment Format: WIRE,
}

message = {
    type: "raw_accounts",
    client: uuid,
    msg_id: uuid,
    payload: {
        batch_size: N,
        batch: [row0, row1, ..., rowN]
    },
}

Example raw account row
row: {
    Bank Name: China Bank #2820,
    Bank ID: 314693, 
    Account Number: 81B86A280,
    Entity ID: 800D8CCF0,
    Entity Name: Corporation #41344, 
}

message = {
    type: "eof",
    client: uuid,
    msg_id: uuid,
}

message = {
    type: "q1_result",
    eof: true | false,
    payload: {
        batch_size: N,
        batch: [row0, row1, ..., rowN]
    },
}

Example row:
row: {
    From Bank: 20,
    Account: 802EABEB0,
    To Bank: 220270,
    Account.1: 80E25DFF0,
    Amount Paid: 9661.410000,
}

message = {
    type: "q2_result",
    eof: true | false,
    payload: {
        batch_size: N,
        batch: [row0, row1, ..., rowN]
    },
}

Example row:
row: {
    From Bank: 20,
    Account: 802EABEB0,
    Bank Name: China Bank #2820,
    Amount Paid: 9661.410000,
}

message = {
    type: "q3_result",
    eof: true | false,
    payload: {
        batch_size: N,
        batch: [row0, row1, ..., rowN]
    },
}

Example row:
row: {
    From Bank: 20,
    Account: 802EABEB0,
    Amount Paid: 9661.410000,
    Payment Format: WIRE,
}

message = {
    type: "q4_result",
    eof: true | false,
    payload: {
        batch_size: N,
        batch: [row0, row1, ..., rowN]
    },
}

Example row:
row: {
    Bank: 20,
    Account: 802EABEB0,
}

message = {
    type: "q5_result",
    eof: true,
    payload: N,
}
"""
