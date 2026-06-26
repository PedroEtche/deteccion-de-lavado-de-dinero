#!/usr/bin/env python3
import time
import random
import subprocess
from datetime import datetime

# Containers protegidos (no se matan). Match por substring sobre el nombre.
EXCLUDE_PATTERNS = ["rabbitmq", "gateway", "client_"]
INTERVAL = 5  # segundos entre kills
COMPOSE_FILE = "docker-compose.yaml"
WAIT_TIMEOUT = 300  # max segundos a esperar a que todo este arriba
VICTIM = 3


def now():
    return datetime.now().strftime("%H:%M:%S")


def running_containers():
    """Nombres de los containers que estan corriendo."""
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
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def total_services():
    """Cuantos servicios define el compose file."""
    result = subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "config", "--services"],
        capture_output=True,
        text=True,
    )
    return len([line for line in result.stdout.splitlines() if line.strip()])


def is_excluded(name):
    for pattern in EXCLUDE_PATTERNS:
        if pattern in name:
            return True
    return False


def wait_for_up():
    """Espera a que todos los servicios del compose esten corriendo."""
    total = total_services()
    print(f"[chaos] Esperando a que arranquen los {total} containers...")
    waited = 0
    while len(running_containers()) < total:
        if waited >= WAIT_TIMEOUT:
            print(f"[chaos] Timeout tras {waited}s. Empiezo igual.")
            return
        time.sleep(2)
        waited += 2
    print("[chaos] Todos arriba. Empieza el caos.")


def main():
    print(f"[chaos] Chaos Monkey iniciado. Intervalo={INTERVAL}s")
    print(f"[chaos] Protegidos: {', '.join(EXCLUDE_PATTERNS)}")
    print("[chaos] Ctrl+C para detener.\n")

    wait_for_up()

    kill_count = 0
    victims = []
    try:
        while True:
            eligible = [c for c in running_containers() if not is_excluded(c)]
            if not eligible:
                print(f"[{now()}] [chaos] No hay containers elegibles. Esperando...")
                time.sleep(INTERVAL)
                continue

            victims = []
            for _ in range(VICTIM):
                victim = random.choice(eligible)
                victims.append(victim)

            for v in victims:
                result = subprocess.run(
                    ["docker", "kill", "--signal=KILL", v],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    kill_count += 1
                    print(
                        f"[{now()}] [chaos] SIGKILL -> {victims}  (kill #{kill_count})"
                    )
                else:
                    print(
                        f"[{now()}] [chaos] Fallo al matar {victims} (quiza ya murio)."
                    )

            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        print(f"\n[chaos] Detenido. Total asesinados: {kill_count}.")


if __name__ == "__main__":
    main()
