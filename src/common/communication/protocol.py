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


def read_csv_batches(csv_path, batch_size, stream):
    """
    Generador que lee un CSV y hace yield de batches (listas de TransactionRow o
    AccountRow según `stream`). Es la mitad "lectora" de send_csv: no toca la red,
    así el cliente puede asignar un msg_id por batch y decidir cuáles enviar.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    if stream == STREAM_TRANSACTIONS:
        field_map = _TRANSACTION_FIELD_MAP
        row_cls = TransactionRow
    elif stream == STREAM_ACCOUNTS:
        field_map = _ACCOUNT_FIELD_MAP
        row_cls = AccountRow
    else:
        raise ValueError(f"Unknown stream: {stream}")

    with open(csv_path, "r", encoding=_ENCODING, newline="") as handle:
        reader = csv.DictReader(handle)

        batch = []
        for raw_row in reader:
            batch.append(_map_row(raw_row, field_map, row_cls))
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch


def build_stream_message(stream, *, client, msg_id, batch, sender):
    """Construye el mensaje raw correspondiente al `stream` (transactions o
    accounts). Despacha al builder de internal.py según el tipo de stream."""
    if stream == STREAM_TRANSACTIONS:
        build_fn = build_raw_transactions_message
    elif stream == STREAM_ACCOUNTS:
        build_fn = build_raw_accounts_message
    else:
        raise ValueError(f"Unknown stream: {stream}")
    return build_fn(client=client, msg_id=msg_id, batch=batch, sender=sender)


def send_csv(sock, csv_path, batch_size, stream, *, sender):
    """
    Lee un CSV, lo convierte a TransactionRow/AccountRow según `stream`
    y lo envía en batches usando el protocolo de internal.py.

    - sender: identificador estable del emisor (ej. "client"); obligatorio
              porque serialize() lo exige en todo mensaje que sale al cable.
    """
    for batch in read_csv_batches(csv_path, batch_size, stream):
        msg = build_stream_message(
            stream, client=None, msg_id=None, batch=batch, sender=sender
        )
        sock.send_bytes(serialize(msg))


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
