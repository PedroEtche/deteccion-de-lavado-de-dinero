SHELL := /bin/bash
PWD := $(shell pwd)

up:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

test:
	# Procesar los datos de forma secuencial y compararlo con nuestro resultado
.PHONY: test

q1_switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, set de datos LI-Small, una sola replica de cada elemento"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/q1/$${option}.yaml docker-compose.yaml
.PHONY: q1_switch

q3_switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, set de datos LI-Small, una sola replica de cada elemento"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/q3/$${option}.yaml docker-compose.yaml
.PHONY: q3_switch

# Genera un sample chico del dataset para iterar rapido. Por default toma las
# primeras SAMPLE_ROWS filas de HI-Large_Trans.csv (head es O(filas pedidas),
# no del archivo, asi que samplear desde Large o Small cuesta lo mismo).
#
# CAVEAT Q3/Q4: head devuelve un prefijo contiguo del archivo. Para Q1 y Q2
# (filtros por fila) sirve. Q3 ([2022-09-06, 2022-09-15]) y Q4
# ([2022-09-01, 2022-09-05]) necesitan filas en ese rango de fechas; si el
# prefijo no las incluye, los resultados van a ser vacios. Para esas queries
# usar otra estrategia, por ejemplo: grep '2022-09-' SOURCE | head -n N.
SAMPLE_ROWS ?= 50000
SOURCE_DATASET ?= /home/matias/facultad/HI-Large_Trans.csv
DATA_DIR := ./data
SAMPLE_FILE := $(DATA_DIR)/sample.csv

sample:
	@mkdir -p $(DATA_DIR)
	@echo "Sampling $(SAMPLE_ROWS) rows from $(SOURCE_DATASET) into $(SAMPLE_FILE)"
	@head -n $$(( $(SAMPLE_ROWS) + 1 )) $(SOURCE_DATASET) > $(SAMPLE_FILE)
.PHONY: sample

DEMO_COMPOSE := docker-compose.q1.yaml

# Levanta el pipeline Q1 end-to-end: rabbit + gateway + filters + cliente.
# Si data/sample.csv no existe, genera uno con `make sample` antes de buildear.
demo: $(SAMPLE_FILE)
	docker compose -f $(DEMO_COMPOSE) up --build --remove-orphans
.PHONY: demo

$(SAMPLE_FILE):
	@$(MAKE) sample

# Q5 necesita transacciones del 2022-09-01 al 2022-09-05. El sample generico
# es un head del dataset y empieza en agosto, asi que Q5 necesita su propio
# sample filtrado por fecha.
SAMPLE_Q5_FILE := $(DATA_DIR)/sample_q5.csv
SAMPLE_Q5_ROWS ?= 50000

sample-q5:
	@mkdir -p $(DATA_DIR)
	@echo "Sampling $(SAMPLE_Q5_ROWS) rows with 2022-09-0[1-5] dates from $(SOURCE_DATASET)"
	@head -1 $(SOURCE_DATASET) > $(SAMPLE_Q5_FILE)
	@grep -E "^2022/09/0[1-5]" $(SOURCE_DATASET) | head -n $(SAMPLE_Q5_ROWS) >> $(SAMPLE_Q5_FILE)
	@echo "Done: $$(wc -l < $(SAMPLE_Q5_FILE)) rows (including header)"
.PHONY: sample-q5

$(SAMPLE_Q5_FILE):
	@$(MAKE) sample-q5

demo-down:
	docker compose -f $(DEMO_COMPOSE) down -t 5
.PHONY: demo-down

DEMO_Q2_COMPOSE := docker-compose.q2.yaml

# Levanta el pipeline Q2 end-to-end: rabbit + gateway + filter_usd + group +
# aggregator + join + cliente. Requiere data/sample.csv (lo genera automaticamente).
demo-q2: $(SAMPLE_FILE)
	docker compose -f $(DEMO_Q2_COMPOSE) up --build --remove-orphans
.PHONY: demo-q2

demo-q2-down:
	docker compose -f $(DEMO_Q2_COMPOSE) down -t 5
.PHONY: demo-q2-down

# Runner orientado a "correr y comparar": corre el escenario, espera a que los
# clientes terminen, vuelca logs por contenedor, meta (duracion + exit codes) y
# resumen de resultados a results/<timestamp>_<escenario>/.
#
# Override de parametros via env:
#   BATCH_SIZE=1000 make run-q2
BATCH_SIZE ?= 500

run-q1: $(SAMPLE_FILE)
	BATCH_SIZE=$(BATCH_SIZE) ./scripts/run.sh q1
.PHONY: run-q1

run-q2: $(SAMPLE_FILE)
	BATCH_SIZE=$(BATCH_SIZE) ./scripts/run.sh q2
.PHONY: run-q2

run-q5: $(SAMPLE_Q5_FILE)
	BATCH_SIZE=$(BATCH_SIZE) ./scripts/run.sh q5
.PHONY: run-q5

# Corre q1 y q2 en serie (cada uno en su propio results/<timestamp>_<escenario>/).
# Cuando aparezca docker-compose.all.yaml con todas las queries en paralelo,
# agregar `run-all-parallel` como ./scripts/run.sh all.
run-all: run-q1 run-q2 run-q5
.PHONY: run-all

# Corre el escenario dos veces y verifica que el resultado (sin UUIDs ni
# tiempos) sea byte-igual entre corridas.
verify-q1: $(SAMPLE_FILE)
	BATCH_SIZE=$(BATCH_SIZE) ./scripts/verify.sh q1
.PHONY: verify-q1

verify-q2: $(SAMPLE_FILE)
	BATCH_SIZE=$(BATCH_SIZE) ./scripts/verify.sh q2
.PHONY: verify-q2

verify-q5: $(SAMPLE_Q5_FILE)
	BATCH_SIZE=$(BATCH_SIZE) ./scripts/verify.sh q5
.PHONY: verify-q5

# Lista las ultimas 10 corridas, ordenadas por fecha desc.
results:
	@if [[ -d results ]]; then \
		ls -1t results/ 2>/dev/null | head -10 | while read d; do \
			meta="results/$$d/meta.txt"; \
			if [[ -f $$meta ]]; then \
				dur=$$(grep -E '^duration_seconds:' $$meta | awk '{print $$2}'); \
				ec=$$(grep -E '^exit_code:' $$meta | head -1 | awk '{print $$2}'); \
				printf "%-40s  %3ss  exit=%s\n" "$$d" "$$dur" "$$ec"; \
			else \
				printf "%-40s  (incomplete)\n" "$$d"; \
			fi; \
		done; \
	else \
		echo "No results yet. Run: make run-q1 / make run-q2 / make run-all"; \
	fi
.PHONY: results

# Muestra el summary de una corrida especifica.
#   make show RUN=20260528_105334_q2
show:
	@if [[ -z "$(RUN)" ]]; then echo "Usage: make show RUN=<dir>"; exit 1; fi
	@cat results/$(RUN)/meta.txt
	@echo ""
	@cat results/$(RUN)/summary.txt
.PHONY: show

# Muestra el summary de la ultima corrida.
show-last:
	@last=$$(ls -1t results/ 2>/dev/null | head -1); \
	if [[ -z "$$last" ]]; then echo "No results yet."; exit 1; fi; \
	echo "==> $$last"; echo ""; \
	cat results/$$last/meta.txt; \
	echo ""; \
	cat results/$$last/summary.txt
.PHONY: show-last

# Limpia containers/volumes de los escenarios sin tocar results/.
run-clean:
	-docker compose -f docker-compose.q1.yaml down -t 3 2>/dev/null
	-docker compose -f docker-compose.q2.yaml down -t 3 2>/dev/null
	-docker compose -f docker-compose.q5.yaml down -t 3 2>/dev/null
.PHONY: run-clean
