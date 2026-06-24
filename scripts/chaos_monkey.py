#!/usr/bin/env python3
"""
Chaos Monkey: mata containers al azar (con SIGKILL) para probar tolerancia a fallos.

Cada INTERVAL segundos elige un container vivo al azar (que NO matchee ninguno de
los patrones de EXCLUDE_PATTERNS) y le manda SIGKILL. Corre en loop hasta Ctrl+C.

Uso:
    python3 scripts/chaos_monkey.py
    python3 scripts/chaos_monkey.py --wait      # espera a que todo este arriba primero
    INTERVAL=5 python3 scripts/chaos_monkey.py

Para cambiar que containers se ven afectados, edita EXCLUDE_PATTERNS abajo.
El match es por substring: "client_" protege client_0/1/2, etc.
"""

import os
import sys
import time
import random
import subprocess
from datetime import datetime

# ----------------------------- CONFIGURACION --------------------------------
# Containers protegidos (no se matan). Match por substring sobre el nombre.
EXCLUDE_PATTERNS = ["rabbitmq", "gateway", "client_", "q3_historical_filter", "join"]

# Segundos entre kills.
INTERVAL = int(os.environ.get("INTERVAL", "1"))

# Compose file del proyecto.
COMPOSE_FILE = os.environ.get("COMPOSE_FILE", "docker-compose.yaml")

# Maximo de segundos a esperar en modo --wait (0 = esperar para siempre).
WAIT_TIMEOUT = int(os.environ.get("WAIT_TIMEOUT", "300"))
# ----------------------------------------------------------------------------


def now():
    """Devuelve la hora actual como texto, para los logs."""
    return datetime.now().strftime("%H:%M:%S")


def running_containers():
    """Devuelve la lista de nombres de containers que estan corriendo."""
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            COMPOSE_FILE,
            "ps",
            "--status",
            "running",
            "--format",
            "{{.Name}}",
        ],
        capture_output=True,
        text=True,
    )
    names = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line:
            names.append(line)
    return names


def total_services():
    """Devuelve cuantos servicios define el compose file."""
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "config", "--services"],
        capture_output=True,
        text=True,
    )
    count = 0
    for line in result.stdout.splitlines():
        if line.strip():
            count += 1
    return count


def is_excluded(name):
    """True si el nombre matchea algun patron de exclusion."""
    for pattern in EXCLUDE_PATTERNS:
        if pattern in name:
            return True
    return False


def wait_for_up():
    """Espera a que todos los servicios del compose esten corriendo."""
    total = total_services()
    if total == 0:
        print(
            "[chaos] No se pudieron leer los servicios de",
            COMPOSE_FILE,
            "- sigo igual.",
        )
        return

    print(f"[chaos] Esperando a que arranquen los {total} containers...")
    waited = 0
    while True:
        running = len(running_containers())
        if running >= total:
            print(f"[chaos] Todos arriba ({running}/{total}). Empieza el caos.")
            return
        if WAIT_TIMEOUT > 0 and waited >= WAIT_TIMEOUT:
            print(
                f"[chaos] Timeout esperando ({running}/{total} arriba tras {waited}s). Empiezo igual."
            )
            return
        time.sleep(2)
        waited += 2


def kill(name):
    """Manda SIGKILL al container. Devuelve True si lo logro."""
    result = subprocess.run(
        ["docker", "kill", "--signal=KILL", name],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def main():
    print(
        f"[chaos] Chaos Monkey iniciado. Intervalo={INTERVAL}s  Compose={COMPOSE_FILE}"
    )
    print(f"[chaos] Protegidos (substring): {', '.join(EXCLUDE_PATTERNS)}")
    print("[chaos] Ctrl+C para detener.")
    print()

    if "--wait" in sys.argv:
        wait_for_up()

    kill_count = 0
    try:
        while True:
            # Refrescar la lista de containers vivos en cada vuelta.
            eligible = [c for c in running_containers() if not is_excluded(c)]

            if not eligible:
                print(f"[{now()}] [chaos] No hay containers elegibles. Esperando...")
                time.sleep(INTERVAL)
                continue

            victim = random.choice(eligible)
            if kill(victim):
                kill_count += 1
                print(
                    f"[{now()}] [chaos] SIGKILL -> {victim}  (kill #{kill_count}, elegibles={len(eligible)})"
                )
            else:
                print(f"[{now()}] [chaos] Fallo al matar {victim} (quiza ya murio).")

            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print()
        print(f"[chaos] Detenido. Total de containers asesinados: {kill_count}.")


if __name__ == "__main__":
    main()
