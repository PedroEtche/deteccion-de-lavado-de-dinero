#!/usr/bin/env python3
"""
chaos.py - Chaos monkey para el sistema de deteccion de fallas (heartbeat).

Mata contenedores del cluster de forma aleatoria y verifica que el heartbeat
los detecta y los vuelve a levantar, midiendo el tiempo de recuperacion.

El cluster debe estar corriendo antes de ejecutar este script
(make fail_recovery_up).
"""

import random
import subprocess
import time
from datetime import datetime

# --- Configuracion -----------------------------------------------------------

N_NODES = 10  # cantidad de nodos del cluster
TARGETS = [f"node{i}" for i in range(1, N_NODES + 1)]  # contenedores objetivo
ROUNDS = 30  # rondas de caos
MIN_SLEEP = 3  # espera minima entre rondas (segundos)
MAX_SLEEP = 8  # espera maxima entre rondas (segundos)
RECOVERY_TIMEOUT = 30  # tiempo maximo para considerar recuperado (segundos)
# MULTI = 5  # cuantos contenedores matar por ronda
VERIFY_LOGS = True  # confirmar recuperacion via logs del cluster

COMPOSE_FILE = "src/common/fail_recovery/docker-compose.yaml"

# --- Colores -----------------------------------------------------------------

GREEN = "\033[1;32m"
RED = "\033[1;31m"
YELLOW = "\033[1;33m"
RESET = "\033[0m"

# --- Helpers -----------------------------------------------------------------


def log(msg: str):
    print(f"{datetime.now().strftime('%H:%M:%S')} {msg}")


def info(msg: str):
    print(f"{YELLOW}{msg}{RESET}")


def container_running(container: str) -> bool:
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "true"


def kill_container(container: str):
    result = subprocess.run(
        ["docker", "kill", "--signal", "SIGKILL", container],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        log(f"  killed {container} (SIGKILL)")
    else:
        log(f"  no se pudo matar {container} (ya estaba caido?)")


def wait_recovery(container: str) -> int | None:
    """Espera hasta que el contenedor vuelva a estar running.
    Retorna los segundos que tardo, o None si supero el timeout."""
    start = time.monotonic()
    while True:
        if container_running(container):
            return int(time.monotonic() - start)
        elapsed = time.monotonic() - start
        if elapsed >= RECOVERY_TIMEOUT:
            return None
        time.sleep(1)


def verify_via_logs(container: str, since_seconds: int) -> bool:
    """Confirma en los logs del cluster que el heartbeat fue quien reinicio el contenedor."""
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            COMPOSE_FILE,
            "logs",
            "--since",
            f"{since_seconds}s",
        ],
        capture_output=True,
        text=True,
    )
    return f"Successfully restarted {container}" in result.stdout


# --- Registro de resultados --------------------------------------------------


def record(container: str, elapsed: int | None, stats: dict):
    stats["total"] += 1

    if elapsed is None:
        stats["fail"] += 1
        print(f"  {RED}FAIL{RESET}  {container} no recupero en {RECOVERY_TIMEOUT}s")
        return

    log_status = "logs no verificados"
    if VERIFY_LOGS:
        if verify_via_logs(container, elapsed + 5):
            log_status = "heartbeat confirmado"
        else:
            log_status = "WARN: sin confirmacion en logs"

    stats["ok"] += 1
    stats["times"].append(elapsed)
    print(f"  {GREEN}OK{RESET}    {container} recuperado en {elapsed}s ({log_status})")


# --- Loop de caos ------------------------------------------------------------


def run_chaos():
    stats = {"total": 0, "ok": 0, "fail": 0, "times": []}

    info(f"Chaos monkey: {ROUNDS} rondas, MULTI={MULTI}, timeout={RECOVERY_TIMEOUT}s")
    info(f"Objetivos: {TARGETS}")
    print()

    for round_num in range(1, ROUNDS + 1):
        targets = random.sample(TARGETS, min(MULTI, len(TARGETS)))
        log(f"== Ronda {round_num}/{ROUNDS}: matando {targets} ==")

        for container in targets:
            kill_container(container)

        for container in targets:
            elapsed = wait_recovery(container)
            record(container, elapsed, stats)

        if round_num < ROUNDS:
            nap = random.randint(MIN_SLEEP, MAX_SLEEP)
            log(f"  durmiendo {nap}s antes de la proxima ronda")
            time.sleep(nap)

        print()

    # --- Resumen -------------------------------------------------------------

    ok, fail, total = stats["ok"], stats["fail"], stats["total"]
    times = stats["times"]
    avg = int(sum(times) / len(times)) if times else 0
    tmin = min(times) if times else 0
    tmax = max(times) if times else 0

    print("=" * 54)
    print(f"Recuperaciones: {ok}/{total} OK, {fail} FAIL")
    print(f"Tiempos de recuperacion (s): min={tmin} avg={avg} max={tmax}")

    if fail == 0 and total > 0:
        print(
            f"\n{GREEN}PASS{RESET}  el heartbeat detecto y revivio todos los contenedores\n"
        )
        return 0
    else:
        print(f"\n{RED}FAIL{RESET}  hubo {fail} recuperaciones fallidas\n")
        return 1


if __name__ == "__main__":
    for i in range(5, 10):
        MULTI = i
        run_chaos()
