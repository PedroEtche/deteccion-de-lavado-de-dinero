SHELL := /bin/bash
PWD := $(shell pwd)

up:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 15
	docker compose -f docker-compose.yaml down
	# rm -f results/clients/client_*/q1.csv results/clients/client_*/q2.csv results/clients/client_*/q3.csv results/clients/client_*/q4.csv results/clients/client_*/q5.csv
	rm -rf results/clients/client_*
	rm -f persisted_state/*
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

test:
	-python3 scripts/compare_results.py q1
	-python3 scripts/compare_results.py q2
	-python3 scripts/compare_results.py q3
	-python3 scripts/compare_results.py q4
	-python3 scripts/compare_results.py q5
.PHONY: test

debug:
	@echo "Levanto solo Rabbit + Gateawy + cliente para ver si los mensajes llegan"
	cp ./scenarios/debug-compose.yaml docker-compose.yaml
.PHONY: debug


q1_switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, set de datos de prueba (small_trans), una sola replica de cada elemento"
	@echo "2) Un cliente, set de datos de prueba (small_trans), una sola replica de cada elemento y el router de transacciones"
	@echo "3) Un cliente, set de datos de prueba (small_trans), 2 replicas del filtro de q1  y fail detection"
	@echo "4) Un cliente, set de datos de prueba (LI-Small), 2 replicas del filtro de q1  y fail detection"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/q1/$${option}.yaml docker-compose.yaml
.PHONY: q1_switch

q1_test_fixed:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	@echo "Waiting for client_0 to finish..."
	@docker wait client_0
	python3 scripts/compare_results.py q1
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: q1_test_fixed

q2_switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, set de datos de prueba (small_trans), una sola replica de cada elemento"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/q2/$${option}.yaml docker-compose.yaml
.PHONY: q2_switch

q2_test_fixed:
	cp ./scenarios/q2/1.yaml docker-compose.yaml
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	@echo "Waiting for client_0 to finish..."
	@docker wait client_0
	python3 scripts/compare_results.py q2
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: q2_test_fixed

q3_switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, set de datos de prueba (small_trans), una sola replica de cada elemento"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/q3/$${option}.yaml docker-compose.yaml
.PHONY: q3_switch

q3_test_fixed:
	cp ./scenarios/q3/1.yaml docker-compose.yaml
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	@echo "Waiting for client_0 to finish..."
	@docker wait client_0
	python3 scripts/compare_results.py q3
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: q3_test_fixed

q4_switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, set de datos LI-Small, una sola replica de cada elemento"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/q4/$${option}.yaml docker-compose.yaml
.PHONY: q4_switch

q4_test_fixed:
	cp ./scenarios/q4/1.yaml docker-compose.yaml
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	@echo "Waiting for client_0 to finish..."
	@docker wait client_0
	python3 scripts/compare_results.py q4
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: q4_test_fixed

q5_switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, set de datos de prueba (small_trans), una sola replica de cada elemento"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/q5/$${option}.yaml docker-compose.yaml
.PHONY: q5_switch

q5_test_fixed:
	rm -f results/clients/client_0/q5.csv
	cp ./scenarios/q5/1.yaml docker-compose.yaml
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	@echo "Waiting for client_0 to finish..."
	@docker wait client_0
	-python3 scripts/compare_results.py q5
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: q5_test_fixed

all_switch:
	@echo Escenarios de prueba:
	@echo "1) Pruebas con datos fixed: 1 cliente y 1 worker de cada"
	@echo "2) Pruebas con datos fixed: 3 clientes y 1 worker de cada"
	@echo "3) Pruebas con datos fixed: 1 cliente y 2 workers"
	@echo "4) Pruebas con datos fixed: 3 cliente y 2 workers"
	@echo "5) Pruebas con datos fixed: 3 cliente, 2 workers y longitud de batches pequeñas"
	@echo "6) Pruebas con datos LI-Small: 3 cliente, 2 workers"
	@echo "7) Pruebas con datos Li-Medium: 3 cliente, 2 workers"
	@echo "8) Pruebas con datos fixed: 3 cliente, 3 workers de cada uno"
	@echo "9) Pruebas con datos LI-Small: 3 cliente, 3 workers de cada uno"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/all/$${option}.yaml docker-compose.yaml
.PHONY: q5_switch
