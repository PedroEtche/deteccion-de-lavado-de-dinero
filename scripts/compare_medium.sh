#!/usr/bin/env bash
#
# Compara los resultados de cada cliente contra el resultado esperado del set
# "medium". Como el set es grande, solo corre un cliente a la vez.
#
#   - resultados del cliente:  results/clients/client_N/qN.csv
#   - resultados esperados:    results/medium/qN.csv
#
# A cada archivo se le hace 'sort' (el orden de las filas no importa) y despues
# 'diff' contra el esperado, igual que en el workflow de bash a mano.
#
# Salida:
#   PASS si todos los clientes coinciden con lo esperado.
#   FAIL si algun archivo no coincide, indicando que archivo y en que difiere.
#
# Exit codes:
#   0  todos coinciden
#   1  alguna diferencia o archivo faltante

MEDIUM_DIR="results/medium"
CLIENTS_DIR="results/clients"
QUERIES="q1 q2 q3 q4 q5"

# Carpeta temporal para guardar los archivos ordenados.
SORT_DIR="results/medium-sort"
mkdir -p "$SORT_DIR"

# Ordenamos una vez los esperados.
for q in $QUERIES; do
    if [ -f "$MEDIUM_DIR/$q.csv" ]; then
        sort "$MEDIUM_DIR/$q.csv" > "$SORT_DIR/$q-esperado.csv"
    fi
done

# Buscamos las carpetas de clientes.
clients=$(ls -d "$CLIENTS_DIR"/client_* 2>/dev/null)
if [ -z "$clients" ]; then
    echo "FAIL  no se encontraron clientes en $CLIENTS_DIR/client_*"
    exit 1
fi

overall_ok=1

for client in $clients; do
    # Nombre del cliente, p.ej. "client_0".
    name=$(basename "$client")

    for q in $QUERIES; do
        cliente_csv="$client/$q.csv"
        esperado_csv="$SORT_DIR/$q-esperado.csv"

        # Si no se espera salida para esta query, la salteamos.
        if [ ! -f "$esperado_csv" ]; then
            continue
        fi

        # El cliente deberia tener el archivo si se espera salida.
        if [ ! -f "$cliente_csv" ]; then
            echo "FAIL  $name/$q.csv  falta el archivo"
            overall_ok=0
            continue
        fi

        # Ordenamos el resultado del cliente y lo comparamos.
        cliente_sort="$SORT_DIR/$name-$q.csv"
        sort "$cliente_csv" > "$cliente_sort"

        diferencia=$(diff "$cliente_sort" "$esperado_csv")
        if [ -z "$diferencia" ]; then
            echo "PASS  $name/$q.csv"
        else
            echo "FAIL  $name/$q.csv no coincide con lo esperado:"
            echo "$diferencia"
            overall_ok=0
        fi
    done
done

if [ "$overall_ok" -eq 1 ]; then
    exit 0
else
    exit 1
fi
