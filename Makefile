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

demo-down:
	docker compose -f $(DEMO_COMPOSE) down -t 5
.PHONY: demo-down
