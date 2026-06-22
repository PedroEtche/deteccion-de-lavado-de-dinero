import csv

from .internal import (
    AccountRow,
    TransactionRow,
    build_eof_message,
    build_raw_accounts_message,
    build_raw_transactions_message,
    deserialize,
    serialize,
)

_ENCODING = "utf-8"
_IGNORED_COLUMNS = ("Is Laundering",)

# Column name → TransactionRow field
_TRANSACTION_FIELD_MAP = {
    "Timestamp": "timestamp",
    "From Bank": "from_bank",
    "Account": "from_account",
    "To Bank": "to_bank",
    "Account.1": "to_account",
    "Amount Received": "amount_received",
    "Receiving Currency": "receiving_currency",
    "Amount Paid": "amount_paid",
    "Payment Currency": "payment_currency",
    "Payment Format": "payment_format",
}

# Column name → AccountRow field
_ACCOUNT_FIELD_MAP = {
    "Bank Name": "bank_name",
    "Bank ID": "bank_id",
    "Account Number": "account_number",
    "Entity ID": "entity_id",
    "Entity Name": "entity_name",
}

STREAM_TRANSACTIONS = 1
STREAM_ACCOUNTS = 2


def send_csv(sock, csv_path, batch_size, stream, *, sender):
    """
    Lee un CSV, lo convierte a TransactionRow/AccountRow según `stream`
    y lo envía en batches usando el protocolo de internal.py.

    - sender: identificador estable del emisor (ej. "client"); obligatorio
              porque serialize() lo exige en todo mensaje que sale al cable.
    - client: identificador del cliente (requerido por build_raw_* )
    - msg_id_fn: callable sin args que genera un msg_id por batch.
                 Por defecto usa uuid.uuid4().
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    if stream == STREAM_TRANSACTIONS:
        field_map = _TRANSACTION_FIELD_MAP
        row_cls = TransactionRow
        build_fn = build_raw_transactions_message
    elif stream == STREAM_ACCOUNTS:
        field_map = _ACCOUNT_FIELD_MAP
        row_cls = AccountRow
        build_fn = build_raw_accounts_message
    else:
        raise ValueError(f"Unknown stream: {stream}")

    with open(csv_path, "r", encoding=_ENCODING, newline="") as handle:
        reader = csv.DictReader(handle)

        batch = []
        for raw_row in reader:
            row = _map_row(raw_row, field_map, row_cls)
            batch.append(row)
            if len(batch) >= batch_size:
                _send_batch(sock, batch, build_fn, sender)
                batch = []

        if batch:
            _send_batch(sock, batch, build_fn, sender)


def send_eof(sock, *, client=None, msg_id=None, sender):
    """Envía un mensaje EOF al otro extremo."""
    msg = build_eof_message(client=client, msg_id=msg_id, sender=sender)
    sock.send_bytes(serialize(msg))


def receive_streams(sock):
    """
    Generador que recibe mensajes del socket y hace yield de (stream, batch)
    hasta recibir un EOF.
    """
    while True:
        raw = sock.recv_bytes()
        msg = deserialize(raw)

        msg_type = msg["type"]

        if msg_type == "eof":
            return

        if msg_type == "raw_transactions":
            yield STREAM_TRANSACTIONS, msg["payload"]["batch"]
        elif msg_type == "raw_accounts":
            yield STREAM_ACCOUNTS, msg["payload"]["batch"]
        else:
            raise ValueError(f"Unknown message type: {msg_type}")


# --------------------------------------------------------------------
# HELPERS
# --------------------------------------------------------------------


def _map_row(raw_row, field_map, row_cls):
    """Convierte un dict CSV crudo a la dataclass correspondiente,
    ignorando las columnas en _IGNORED_COLUMNS."""
    kwargs = {}
    for csv_col, field_name in field_map.items():
        if csv_col in _IGNORED_COLUMNS:
            continue
        value = raw_row.get(csv_col)
        if value is not None:
            # strip() saca \r (CSV con line-endings Windows), \n y espacios
            # para que ningun campo arrastre basura al pipeline.
            value = value.strip()
        if value:
            kwargs[field_name] = value
    return row_cls(**kwargs)


def _send_batch(sock, batch, build_fn, sender):
    msg = build_fn(client=None, msg_id=None, batch=batch, sender=sender)
    sock.send_bytes(serialize(msg))
