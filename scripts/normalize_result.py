#!/usr/bin/env python3
"""Extrae los `Result N: {...}` del log de un cliente y los emite en forma
canonica: sin campos no-deterministicos (client uuid, msg_id) y con el batch
ordenado por contenido. Dos corridas con los mismos inputs producen output
byte-igual."""
import json
import re
import sys


def normalize_message(msg):
    out = {"type": msg.get("type")}
    payload = msg.get("payload")
    if payload is None:
        return out
    batch = payload.get("batch")
    if isinstance(batch, list):
        sorted_batch = sorted(batch, key=lambda x: json.dumps(x, sort_keys=True))
        out["payload"] = {"batch_size": payload.get("batch_size"), "batch": sorted_batch}
    else:
        out["payload"] = payload
    return out


def main():
    if len(sys.argv) != 2:
        print("Usage: normalize_result.py <client_log_path>", file=sys.stderr)
        sys.exit(2)

    # docker chunks stdout lines at ~16KB so long result JSON can be split
    # across multiple "log lines". Read the whole file and use raw_decode to
    # parse each Result message regardless of newlines (JSON ignores
    # whitespace between tokens).
    with open(sys.argv[1], encoding="utf-8") as fh:
        text = fh.read()

    decoder = json.JSONDecoder()
    messages = []
    for m in re.finditer(r"Result \d+:\s*", text):
        start = m.end()
        try:
            obj, _ = decoder.raw_decode(text, start)
        except json.JSONDecodeError as exc:
            print(f"Warning: skipping unparseable result at offset {start}: {exc}", file=sys.stderr)
            continue
        messages.append(obj)

    normalized = [normalize_message(m) for m in messages]
    json.dump(normalized, sys.stdout, sort_keys=True, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
