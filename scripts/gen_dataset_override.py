#!/usr/bin/env python3
"""Genera un docker-compose override para que cada cliente del scenario use
un dataset de data/datasets/ (por nombre, ej: HI-Small).

Uso: gen_dataset_override.py <compose_file> <dataset_name>
Imprime el override por stdout.

El override, por cada servicio `client*`:
  - monta data/datasets/ (read-only) dentro del container,
  - pisa TRANSACTIONS_PATH y ACCOUNTS_PATH para apuntar a
    {dataset}_Trans.csv y {dataset}_accounts.csv.
"""

import sys
import yaml

if len(sys.argv) != 3:
    print("uso: gen_dataset_override.py <compose_file> <dataset>", file=sys.stderr)
    sys.exit(2)

compose_file, dataset = sys.argv[1], sys.argv[2]
spec = yaml.safe_load(open(compose_file)) or {}
clients = [name for name in spec.get("services", {}) if name.startswith("client")]
if not clients:
    print("no se encontraron servicios client* en el scenario", file=sys.stderr)
    sys.exit(1)

services = {}
for c in clients:
    services[c] = {
        "volumes": ["./data/datasets:/data/datasets:ro"],
        # map-form: pisa estas keys del base (que estaba en list-form)
        "environment": {
            "TRANSACTIONS_PATH": f"/data/datasets/{dataset}_Trans.csv",
            "ACCOUNTS_PATH": f"/data/datasets/{dataset}_accounts.csv",
        },
    }

yaml.safe_dump(
    {"services": services}, sys.stdout, default_flow_style=False, sort_keys=False
)
