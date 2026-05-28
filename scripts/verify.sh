#!/usr/bin/env bash
# Corre el mismo escenario dos veces con los mismos parametros y verifica que
# el resultado normalizado de cada cliente sea byte-igual entre corridas. La
# duracion puede variar; el contenido no.
#
# Uso:
#   ./scripts/verify.sh <escenario>
#   BATCH_SIZE=1000 ./scripts/verify.sh q2

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <scenario>" >&2
    exit 2
fi

scenario="$1"

echo "[verify] Run 1/2 for ${scenario}"
./scripts/run.sh "${scenario}" > /dev/null

echo "[verify] Run 2/2 for ${scenario}"
./scripts/run.sh "${scenario}" > /dev/null

# Two newest result dirs for this scenario, by mtime.
mapfile -t runs < <(ls -1t results/ 2>/dev/null | grep -E "_${scenario}$" | head -2)

if [[ ${#runs[@]} -lt 2 ]]; then
    echo "[verify] Expected 2 result dirs for ${scenario}, got ${#runs[@]}" >&2
    exit 1
fi

new_run="${runs[0]}"
old_run="${runs[1]}"

echo "[verify] Comparing:"
echo "  newer: results/${new_run}"
echo "  older: results/${old_run}"
echo ""

new_dur=$(grep -E '^duration_seconds:' "results/${new_run}/meta.txt" | awk '{print $2}')
old_dur=$(grep -E '^duration_seconds:' "results/${old_run}/meta.txt" | awk '{print $2}')
echo "  durations: ${old_dur}s -> ${new_dur}s"
echo ""

overall=0
shopt -s nullglob
new_norms=( "results/${new_run}"/*.normalized.json )
shopt -u nullglob

if [[ ${#new_norms[@]} -eq 0 ]]; then
    echo "[verify] No normalized result files in newer run" >&2
    exit 1
fi

for new_norm in "${new_norms[@]}"; do
    fname=$(basename "${new_norm}")
    old_norm="results/${old_run}/${fname}"
    if [[ ! -f "${old_norm}" ]]; then
        echo "  ${fname}: MISSING in older run"
        overall=1
        continue
    fi
    if diff -q "${old_norm}" "${new_norm}" > /dev/null; then
        size=$(wc -c < "${new_norm}")
        echo "  ${fname}: IDENTICAL (${size} bytes)"
    else
        echo "  ${fname}: DIFFERS"
        diff "${old_norm}" "${new_norm}" | head -30
        overall=1
    fi
done

echo ""
if [[ $overall -eq 0 ]]; then
    echo "[verify] PASS: ${scenario} is deterministic"
else
    echo "[verify] FAIL: ${scenario} results differ between runs"
fi
exit "${overall}"
