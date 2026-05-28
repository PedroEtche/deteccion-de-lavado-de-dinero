#!/usr/bin/env bash
# Corre un escenario (q1, q2, q5, ...) y vuelca meta + logs + resumen a results/.
#
# Uso:
#   ./scripts/run.sh <escenario>
#   BATCH_SIZE=1000 CLIENTS=2 WORKERS=3 ./scripts/run.sh q5
#
# Variables de entorno:
#   BATCH_SIZE    tamaño de cada batch de transacciones (default: 500)
#   CLIENTS       instancias del cliente que envian datos en paralelo (default: 1)
#   WORKERS       replicas de las etapas stateless del pipeline (default: 1)
#                 Para Q1/Q5: escala filtros y currency converter.
#                 Para Q2: escala filtros y group; aggregator/join quedan en 1.
#                 Ver Makefile para detalles sobre EXPECTED_EOFS al escalar Q2.
#
# Estructura del output (results/<timestamp>_<escenario>/):
#   meta.txt           # parametros, duracion, exit codes
#   summary.txt        # resultados por cliente (batch_size + EOF)
#   logs/<svc>.log     # logs por contenedor (todos los replicas combinados)
#   logs/_startup.log  # output combinado de `compose up`

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <scenario>" >&2
    echo "Available scenarios:" >&2
    ls docker-compose.*.yaml 2>/dev/null | sed -E 's/docker-compose\.(.+)\.yaml/  \1/' >&2
    exit 2
fi

scenario="$1"
compose_file="docker-compose.${scenario}.yaml"

if [[ ! -f "$compose_file" ]]; then
    echo "Error: $compose_file does not exist" >&2
    exit 2
fi

batch_size="${BATCH_SIZE:-500}"
clients="${CLIENTS:-1}"
workers="${WORKERS:-1}"
timestamp="$(date +%Y%m%d_%H%M%S)"
result_dir="results/${timestamp}_${scenario}"
mkdir -p "${result_dir}/logs"

cat > "${result_dir}/meta.txt" <<EOF
scenario: ${scenario}
compose_file: ${compose_file}
BATCH_SIZE: ${batch_size}
CLIENTS: ${clients}
WORKERS: ${workers}
start: $(date -Iseconds)
EOF

echo "[$(date +%H:%M:%S)] ${scenario} (BATCH_SIZE=${batch_size} CLIENTS=${clients} WORKERS=${workers}) -> ${result_dir}"

# Avoid stale containers from a previous interrupted run.
docker compose -f "${compose_file}" down -t 3 >/dev/null 2>&1 || true

start_epoch=$(date +%s)

# Build --scale args: always scale client_0 to $CLIENTS, plus per-scenario
# worker services to $WORKERS.
scale_args="--scale client_0=${clients}"

# Per-scenario scalable stateless stages. Stateful stages (aggregators, join)
# are intentionally left at 1; scaling them requires adjusting EXPECTED_EOFS
# in the compose file (see Makefile comments).
case "${scenario}" in
    q1)
        scale_args+=" --scale filter_usd_0=${workers} --scale q1_filter_less_than_fifty_0=${workers}"
        ;;
    q2)
        # filter + group scale freely; aggregator/join need EXPECTED_EOFS=N if scaled
        scale_args+=" --scale filter_usd_0=${workers} --scale q2_group_max_amount_0=${workers}"
        ;;
    q5)
        scale_args+=" --scale filter_q5_date_0=${workers}"
        scale_args+=" --scale filter_q5_wire_ach_0=${workers}"
        scale_args+=" --scale q5_currency_converter_0=${workers}"
        scale_args+=" --scale filter_q5_amount_lt1_0=${workers}"
        ;;
esac

export BATCH_SIZE="${batch_size}"
set +e
# shellcheck disable=SC2086
docker compose -f "${compose_file}" up -d --build --remove-orphans ${scale_args} \
    > "${result_dir}/logs/_startup.log" 2>&1
startup_code=$?
set -e

if [[ $startup_code -ne 0 ]]; then
    echo "Startup failed (exit=${startup_code}). See ${result_dir}/logs/_startup.log" >&2
    {
        echo "end: $(date -Iseconds)"
        echo "duration_seconds: $(( $(date +%s) - start_epoch ))"
        echo "exit_code: ${startup_code}"
        echo "status: startup_failed"
    } >> "${result_dir}/meta.txt"
    exit $startup_code
fi

# Wait for every replica of every client_* service to exit.
# With --scale client_0=N, `ps -a -q client_0` returns N container IDs.
client_services=$(docker compose -f "${compose_file}" config --services | grep -E '^client_' || true)
overall_exit=0
client_exits=""
for svc in $client_services; do
    mapfile -t container_ids < <(docker compose -f "${compose_file}" ps -a -q "$svc" 2>/dev/null || true)
    if [[ ${#container_ids[@]} -eq 0 ]]; then
        echo "Warning: no containers for service ${svc}" >&2
        client_exits+="${svc}: missing"$'\n'
        overall_exit=1
        continue
    fi
    for cid in "${container_ids[@]}"; do
        [[ -z "$cid" ]] && continue
        # Cap at 10 min per client; longer means a hang.
        code=$(timeout 600 docker wait "$cid" 2>/dev/null || echo "timeout")
        client_exits+="${svc}/${cid:0:12}: ${code}"$'\n'
        if [[ "$code" != "0" ]]; then overall_exit=1; fi
    done
done

end_epoch=$(date +%s)
duration=$((end_epoch - start_epoch))

# Dump per-service logs. Avoid --timestamps: docker chunks long stdout lines at
# ~16KB and would inject a fresh timestamp inside the JSON of long result
# messages, corrupting them.
for service in $(docker compose -f "${compose_file}" config --services); do
    docker compose -f "${compose_file}" logs --no-color --no-log-prefix "${service}" \
        > "${result_dir}/logs/${service}.log" 2>&1 || true
done

# Normalize each client service's results into a canonical JSON.
for client_log in "${result_dir}"/logs/client_*.log; do
    [[ -f "$client_log" ]] || continue
    client_name=$(basename "${client_log}" .log)
    python3 scripts/normalize_result.py "${client_log}" \
        > "${result_dir}/${client_name}.normalized.json" 2>/dev/null || true
done

# Build summary.
{
    echo "=== Per-client results ==="
    for client_log in "${result_dir}"/logs/client_*.log; do
        [[ -f "$client_log" ]] || continue
        client_name=$(basename "${client_log}" .log)
        normalized="${result_dir}/${client_name}.normalized.json"
        echo ""
        echo "--- ${client_name} ---"
        if [[ -s "${normalized}" ]]; then
            python3 - "${normalized}" <<'PY'
import json, sys
with open(sys.argv[1]) as fh:
    msgs = json.load(fh)
for i, m in enumerate(msgs, 1):
    t = m.get("type")
    payload = m.get("payload") or {}
    size = payload.get("batch_size")
    batch = payload.get("batch") or []
    if t == "eof":
        print(f"  msg {i}: eof")
        continue
    print(f"  msg {i}: type={t} batch_size={size}")
    head, tail_n = batch[:5], 5
    for row in head:
        print(f"    {json.dumps(row, sort_keys=True)}")
    if len(batch) > len(head) + tail_n:
        print(f"    ... ({len(batch) - len(head) - tail_n} more) ...")
        for row in batch[-tail_n:]:
            print(f"    {json.dumps(row, sort_keys=True)}")
    elif len(batch) > len(head):
        for row in batch[len(head):]:
            print(f"    {json.dumps(row, sort_keys=True)}")
PY
        else
            echo "  (no result messages parsed)"
        fi
    done
} > "${result_dir}/summary.txt"

{
    echo "end: $(date -Iseconds)"
    echo "duration_seconds: ${duration}"
    echo "exit_code: ${overall_exit}"
    echo "client_exits:"
    printf '%s' "${client_exits}"
} >> "${result_dir}/meta.txt"

# Clean teardown so the next run starts fresh.
docker compose -f "${compose_file}" down -t 5 >/dev/null 2>&1 || true

echo ""
echo "[$(date +%H:%M:%S)] Done in ${duration}s (exit=${overall_exit})"
echo ""
echo "=== meta ==="
cat "${result_dir}/meta.txt"
echo ""
echo "=== summary ==="
cat "${result_dir}/summary.txt"
echo ""
echo "Logs: ${result_dir}/logs/"

exit "${overall_exit}"
