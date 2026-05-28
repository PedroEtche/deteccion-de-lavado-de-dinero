#!/usr/bin/env bash
# Corre un escenario (q1, q2, ...) y vuelca meta + logs + resumen a results/.
#
# Uso:
#   ./scripts/run.sh <escenario>           # corre docker-compose.<escenario>.yaml
#   BATCH_SIZE=1000 ./scripts/run.sh q2    # override params via env
#
# Estructura del output (results/<timestamp>_<escenario>/):
#   meta.txt           # parametros, duracion, exit codes
#   summary.txt        # resultados por cliente (batch_size + EOF)
#   logs/<svc>.log     # logs por contenedor
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
timestamp="$(date +%Y%m%d_%H%M%S)"
result_dir="results/${timestamp}_${scenario}"
mkdir -p "${result_dir}/logs"

# Snapshot of overridable params for reproducibility.
cat > "${result_dir}/meta.txt" <<EOF
scenario: ${scenario}
compose_file: ${compose_file}
BATCH_SIZE: ${batch_size}
start: $(date -Iseconds)
EOF

echo "[$(date +%H:%M:%S)] ${scenario} (BATCH_SIZE=${batch_size}) -> ${result_dir}"

# Avoid stale containers from a previous interrupted run.
docker compose -f "${compose_file}" down -t 3 >/dev/null 2>&1 || true

start_epoch=$(date +%s)

export BATCH_SIZE="${batch_size}"
set +e
docker compose -f "${compose_file}" up -d --build --remove-orphans \
    > "${result_dir}/logs/_startup.log" 2>&1
startup_code=$?
set -e

if [[ $startup_code -ne 0 ]]; then
    echo "Startup failed (exit=${startup_code}). See ${result_dir}/logs/_startup.log" >&2
    cat >> "${result_dir}/meta.txt" <<EOF
end: $(date -Iseconds)
duration_seconds: $(( $(date +%s) - start_epoch ))
exit_code: ${startup_code}
status: startup_failed
EOF
    exit $startup_code
fi

# Wait for every client_* container to exit. Sequential wait is fine because
# clients are running in parallel — total wall time is max(client durations).
clients=$(docker compose -f "${compose_file}" config --services | grep -E '^client_' || true)
overall_exit=0
client_exits=""
for client in $clients; do
    container_id=$(docker compose -f "${compose_file}" ps -a -q "$client" 2>/dev/null || true)
    if [[ -z "$container_id" ]]; then
        echo "Warning: no container for service ${client}" >&2
        client_exits+="${client}: missing"$'\n'
        overall_exit=1
        continue
    fi
    # Cap at 10 min per client; longer than that should be considered a hang.
    code=$(timeout 600 docker wait "$container_id" 2>/dev/null || echo "timeout")
    client_exits+="${client}: ${code}"$'\n'
    if [[ "$code" != "0" ]]; then overall_exit=1; fi
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

# Normalize each client's results into a canonical JSON for comparison and
# display.
for client_log in "${result_dir}"/logs/client_*.log; do
    [[ -f "$client_log" ]] || continue
    client_name=$(basename "${client_log}" .log)
    python3 scripts/normalize_result.py "${client_log}" \
        > "${result_dir}/${client_name}.normalized.json" 2>/dev/null || true
done

# Build summary by extracting result-relevant lines from each client.
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
