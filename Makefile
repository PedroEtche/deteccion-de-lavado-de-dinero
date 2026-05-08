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

switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, set de datos chico, una sola replica de cada elemento"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/$${option}.yaml docker-compose.yaml
.PHONY: switch
