#!/usr/bin/env python3
"""
Chaos Monkey Dirigido (vía argumentos):
Mata un contenedor específico de forma persistente.

Uso:
    python3 scripts/chaos_targeted.py --target NOMBRE_CONTAINER --interval 5
"""

import time
import argparse
import subprocess
from datetime import datetime


def now():
    return datetime.now().strftime("%H:%M:%S")


def get_container_name(target_name):
    """Busca el nombre completo del contenedor que contiene target_name."""
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True
    )
    for line in result.stdout.splitlines():
        if target_name in line:
            return line.strip()
    return None


def kill(name):
    print(f"[{now()}] [chaos] Asesinando: {name}...")
    result = subprocess.run(
        ["docker", "kill", "--signal=KILL", name],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="Ataca un contenedor específico.")
    parser.add_argument(
        "--target", required=True, help="Nombre (o substring) del container a atacar"
    )
    parser.add_argument(
        "--interval", type=int, default=5, help="Segundos entre intentos de kill"
    )

    args = parser.parse_args()

    print(f"[chaos] Iniciando modo dirigido.")
    print(f"[chaos] Target: {args.target} | Intervalo: {args.interval}s")
    print("[chaos] Ctrl+C para detener.")

    try:
        while True:
            victim = get_container_name(args.target)

            if victim:
                if kill(victim):
                    print(f"[{now()}] [chaos] SIGKILL exitoso sobre {victim}")
                else:
                    print(f"[{now()}] [chaos] Fallo al matar {victim}")
            else:
                # El target no existe o se está reiniciando
                print(
                    f"[{now()}] [chaos] Target '{args.target}' no encontrado. Esperando..."
                )

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[chaos] Detenido.")


if __name__ == "__main__":
    main()

