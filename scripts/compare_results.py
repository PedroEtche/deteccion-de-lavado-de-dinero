#!/usr/bin/env python3
"""
Compare client result CSV files against an expected fixed result.

Usage:
    compare_results.py <query> [--expected PATH] [--clients-dir PATH]

Exit codes:
    0  all clients match expected
    1  at least one mismatch or missing file
    2  bad arguments
"""

import argparse
import csv
import glob
import sys

# ANSI color codes (suprimidos si stdout/stderr no es una TTY,
# para que los logs en archivo o pipe no se llenen de ruido).
_USE_COLOR = sys.stdout.isatty()
GREEN = "\033[1;32m" if _USE_COLOR else ""
RED = "\033[1;31m" if _USE_COLOR else ""
RESET = "\033[0m" if _USE_COLOR else ""


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            return [], []
        headers = list(reader.fieldnames)
        rows = sorted(
            [dict(row) for row in reader],
            key=lambda r: [r.get(h, "") for h in headers],
        )
    return headers, rows


def compare_csvs(got_path, expected_path):
    try:
        got_headers, got_rows = read_csv(got_path)
    except FileNotFoundError:
        return False, f"client result not found: {got_path}"
    except Exception as exc:
        return False, f"error reading {got_path}: {exc}"

    try:
        exp_headers, exp_rows = read_csv(expected_path)
    except FileNotFoundError:
        return False, f"expected result not found: {expected_path}"
    except Exception as exc:
        return False, f"error reading {expected_path}: {exc}"

    if got_headers != exp_headers:
        return (
            False,
            f"header mismatch\n  got:      {got_headers}\n  expected: {exp_headers}",
        )

    if len(got_rows) != len(exp_rows):
        return False, (
            f"row count mismatch: got {len(got_rows)}, expected {len(exp_rows)}\n"
            + _first_diff(got_rows, exp_rows)
        )

    for i, (g, e) in enumerate(zip(got_rows, exp_rows)):
        if g != e:
            return False, f"row {i + 1} differs:\n  got:      {g}\n  expected: {e}"

    return True, None


def _first_diff(got, expected):
    got_set = {tuple(sorted(r.items())) for r in got}
    exp_set = {tuple(sorted(r.items())) for r in expected}
    missing = [dict(r) for r in (exp_set - got_set)]
    extra = [dict(r) for r in (got_set - exp_set)]
    lines = []
    if missing:
        lines.append(f"  missing rows: {missing[:3]}")
    if extra:
        lines.append(f"  extra rows:   {extra[:3]}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Compare client CSV results against expected output"
    )
    parser.add_argument("query", help="Query name, e.g. q1")
    parser.add_argument(
        "--expected",
        help="Path to the expected result CSV (default: ./results/fixed/<query>.csv)",
    )
    parser.add_argument(
        "--clients-dir",
        default="./results/clients",
        help="Directory containing client_N subdirectories (default: ./results/clients)",
    )
    args = parser.parse_args()

    expected_path = args.expected or f"./results/fixed/{args.query}.csv"
    pattern = f"{args.clients_dir}/client_*/{args.query}.csv"
    client_files = sorted(glob.glob(pattern))

    if not client_files:
        print(f"\n{RED}FAIL{RESET}  no client result files found at {pattern}\n", file=sys.stderr)
        sys.exit(1)

    overall_ok = True
    for client_csv in client_files:
        ok, reason = compare_csvs(client_csv, expected_path)
        if ok:
            print(f"\n{GREEN}PASS{RESET}  {client_csv}\n")
        else:
            print(f"\n{RED}FAIL{RESET}  {client_csv}\n      {reason}\n")
            overall_ok = False

    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
