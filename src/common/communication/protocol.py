import csv
import io


BATCH = 1
EOF = 2

STREAM_TRANSACTIONS = 1
STREAM_ACCOUNTS = 2

_ENCODING = "utf-8"
_IGNORED_COLUMNS = ("Is Laundering",)


def send_csv(sock, csv_path, batch_size, stream):
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    with open(csv_path, "r", encoding=_ENCODING, newline="") as handle:
        reader = csv.reader(handle)
        raw_header = next(reader, None)
        if raw_header is None:
            return

        kept, header_values = _prepare_header(raw_header)
        header_line = _format_row(header_values)

        batch = [header_line]
        for row in reader:
            batch.append(_format_row([row[i] for i in kept]))
            if len(batch) - 1 >= batch_size:
                _send_batch(sock, batch, stream)
                batch = [header_line]
        if len(batch) > 1:
            _send_batch(sock, batch, stream)


def send_eof(sock):
    sock.send_bytes(bytes([EOF]))


def receive_streams(sock):
    while True:
        payload = sock.recv_bytes()
        if not payload:
            raise ConnectionError("Empty frame received")

        msg_type = payload[0]
        body = payload[1:]

        if msg_type == EOF:
            return
        if msg_type == BATCH:
            stream = body[0]
            csv_body = body[1:]
            yield stream, _parse_batch(csv_body)
            continue

        raise ValueError(f"Unknown message type: {msg_type}")


def _prepare_header(raw_header):
    seen = {}
    kept = []
    processed = []
    for idx, name in enumerate(raw_header):
        if name in _IGNORED_COLUMNS:
            continue
        kept.append(idx)
        count = seen.get(name, 0)
        processed.append(name if count == 0 else f"{name}.{count}")
        seen[name] = count + 1
    return kept, processed


def _format_row(values):
    buf = io.StringIO()
    csv.writer(buf).writerow(values)
    return buf.getvalue()


def _send_batch(sock, lines, stream):
    payload = bytes([BATCH, stream]) + "".join(lines).encode(_ENCODING)
    sock.send_bytes(payload)


def _parse_batch(body):
    text = body.decode(_ENCODING)
    return list(csv.DictReader(text.splitlines()))
