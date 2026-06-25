#!/usr/bin/env python3
"""
Compara los resultados de cada cliente contra el resultado fijo esperado.

Uso:
    compare_results.py <query>      # p.ej. compare_results.py q1

Compara cada ./results/clients/client_*/<query>.csv contra
./results/fixed/<query>.csv, como conjunto ordenado de filas.

Exit codes:
    0  todos los clientes coinciden con lo esperado
    1  alguna diferencia o archivo faltante
    2  faltan argumentos
"""

import csv
import glob
import sys

FIXED_DIR = "./results/fixed"
CLIENTS_DIR = "./results/clients"

# Colores ANSI (suprimidos si no es una TTY).
_USE_COLOR = sys.stdout.isatty()
GREEN = "\033[1;32m" if _USE_COLOR else ""
RED = "\033[1;31m" if _USE_COLOR else ""
RESET = "\033[0m" if _USE_COLOR else ""


def read_rows(path):
    """CSV headerless como conjunto ordenado de filas (el orden no importa)."""
    with open(path, newline="", encoding="utf-8") as fh:
        return sorted(tuple(row) for row in csv.reader(fh) if row)


def compare(got_path, expected_path):
    """Compara dos CSV. Devuelve (ok, motivo)."""
    got = read_rows(got_path)
    expected = read_rows(expected_path)

    if len(got) != len(expected):
        return False, f"cantidad de filas: {len(got)} vs {len(expected)} esperadas"
    for i in range(len(got)):
        if got[i] != expected[i]:
            return False, f"fila {i + 1} difiere: got {got[i]} / esperado {expected[i]}"
    return True, None


def main():
    if len(sys.argv) != 2:
        print("uso: compare_results.py <query>", file=sys.stderr)
        sys.exit(2)
    query = sys.argv[1]

    expected_path = f"{FIXED_DIR}/{query}.csv"
    client_files = sorted(glob.glob(f"{CLIENTS_DIR}/client_*/{query}.csv"))
    if not client_files:
        print(f"{RED}FAIL{RESET}  no hay resultados de clientes para {query}")
        sys.exit(1)

    overall_ok = True
    for client_csv in client_files:
        ok, motivo = compare(client_csv, expected_path)
        if ok:
            print(f"{GREEN}PASS{RESET}  {client_csv}")
        else:
            print(f"{RED}FAIL{RESET}  {client_csv}\n      {motivo}")
            overall_ok = False

    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
