#!/usr/bin/env python3
"""
Compara los resultados de los clientes ENTRE SI (sin archivo de referencia).

Toma el primer cliente como referencia y verifica que todos los demas hayan
obtenido la misma respuesta para cada query.

Exit codes:
    0  todos los clientes coinciden entre si
    1  alguna divergencia o archivo faltante de forma inconsistente
"""

import csv
import glob
import os
import sys

QUERIES = ["q1", "q2", "q3", "q4", "q5"]
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


def compare_query(query, client_dirs):
    """Compara una query entre todos los clientes. Devuelve (ok, mensaje)."""
    present = []
    for d in client_dirs:
        if os.path.exists(os.path.join(d, f"{query}.csv")):
            present.append(d)

    # Un archivo ausente solo es valido si falta para TODOS los clientes.
    if not present:
        return True, f"{query}: sin archivo en ningun cliente (OK)"
    if len(present) != len(client_dirs):
        return False, f"{query}: el archivo esta en unos clientes y en otros no"

    # Primer cliente como referencia.
    ref = client_dirs[0]
    ref_rows = read_rows(os.path.join(ref, f"{query}.csv"))

    diferentes = []
    for d in client_dirs[1:]:
        if read_rows(os.path.join(d, f"{query}.csv")) != ref_rows:
            diferentes.append(d)

    if not diferentes:
        return True, f"{query}: {len(client_dirs)} clientes coinciden ({len(ref_rows)} filas)"
    return False, f"{query}: difieren de {ref}: {diferentes}"


def main():
    client_dirs = sorted(glob.glob(f"{CLIENTS_DIR}/client_*"))
    if not client_dirs:
        print(f"{RED}FAIL{RESET}  no se encontraron clientes en {CLIENTS_DIR}/client_*")
        sys.exit(1)
    if len(client_dirs) == 1:
        print(f"{GREEN}OK{RESET}  un solo cliente, nada que comparar")
        sys.exit(0)

    overall_ok = True
    for query in QUERIES:
        ok, msg = compare_query(query, client_dirs)
        tag = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
        print(f"{tag}  {msg}")
        if not ok:
            overall_ok = False

    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
