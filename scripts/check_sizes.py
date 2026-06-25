#!/usr/bin/env python3
"""
Chequeo de consistencia por tamaño de los resultados de cada cliente.

A diferencia de compare_results.py (que compara contra un CSV esperado fila por
fila), este script solo verifica que la CANTIDAD de lineas de cada query coincida
con el tamaño esperado. Pensado para correr con `make test_all`.

Caso especial q5: debe tener exactamente 1 linea y su valor debe ser 9261.

Exit codes:
    0  todos los clientes tienen los tamaños esperados
    1  al menos un cliente no coincide (o falta un archivo que deberia existir)
"""

import glob
import os
import sys

# Cantidad de lineas esperada por query.
EXPECTED_LINES = {
    "q1": 336253,
    "q2": 15698,
    "q3": 663650,
    "q4": 0,
    "q5": 1,
}

# La unica linea de q5 debe valer esto.
Q5_EXPECTED_VALUE = "9261"

CLIENTS_DIR = "./results/clients"

# Colores ANSI (suprimidos si no es una TTY).
_USE_COLOR = sys.stdout.isatty()
GREEN = "\033[1;32m" if _USE_COLOR else ""
RED = "\033[1;31m" if _USE_COLOR else ""
RESET = "\033[0m" if _USE_COLOR else ""


def read_lines(path):
    """Devuelve las lineas no vacias del archivo (lista de strings sin el \\n)."""
    with open(path, encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip()]


def check_query(client_dir, query):
    """Chequea una query de un cliente. Devuelve (ok, mensaje)."""
    path = os.path.join(client_dir, f"{query}.csv")
    expected = EXPECTED_LINES[query]

    if not os.path.exists(path):
        # Un archivo ausente solo es valido cuando el tamaño esperado es 0.
        if expected == 0:
            return True, f"{query}: 0 lineas (sin archivo)"
        return False, f"{query}: falta el archivo {path}"

    try:
        lines = read_lines(path)
    except Exception as exc:
        return False, f"{query}: error leyendo {path}: {exc}"

    if len(lines) != expected:
        return False, f"{query}: {len(lines)} lineas, esperadas {expected}"

    if query == "q5" and lines and lines[0] != Q5_EXPECTED_VALUE:
        return False, f"{query}: valor {lines[0]!r}, esperado {Q5_EXPECTED_VALUE!r}"

    return True, f"{query}: {len(lines)} lineas"


def main():
    client_dirs = sorted(glob.glob(f"{CLIENTS_DIR}/client_*"))

    if not client_dirs:
        print(
            f"\n{RED}FAIL{RESET}  no se encontraron clientes en {CLIENTS_DIR}/client_*\n",
            file=sys.stderr,
        )
        sys.exit(1)

    overall_ok = True
    for client_dir in client_dirs:
        for query in sorted(EXPECTED_LINES):
            ok, msg = check_query(client_dir, query)
            tag = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
            print(f"{tag}  {client_dir}  {msg}")
            if not ok:
                overall_ok = False

    print()
    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
