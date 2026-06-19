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
	# Aca se puede tener un test de las 5 queries a la vez
.PHONY: test

debug:
	@echo "Levanto solo Rabbit + Gateawy + cliente para ver si los mensajes llegan"
	cp ./scenarios/debug-compose.yaml docker-compose.yaml
.PHONY: debug


q1_switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, set de datos de prueba (small_trans), una sola replica de cada elemento"
	@echo "2) Un cliente, set de datos de prueba (small_trans), una sola replica de cada elemento y el router de transacciones"
	@echo "3) Un cliente, set de datos de prueba (LI-Small_Trans), una sola replica de cada elemento, el router de transacciones y un esclavo del filter y join de la q1"
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
	rm -f results/clients/client_0/q5.txt
	cp ./scenarios/q5/1.yaml docker-compose.yaml
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	@echo "Waiting for client_0 to finish..."
	@docker wait client_0
	@got=$$(tr -d '[:space:]' < results/clients/client_0/q5.txt 2>/dev/null); \
	exp=$$(tr -d '[:space:]' < results/fixed/q5.txt); \
	if [ "$$got" = "$$exp" ]; then \
		printf '\n\033[1;32mPASS\033[0m  q5 (count=%s)\n\n' "$$got"; \
	else \
		printf '\n\033[1;31mFAIL\033[0m  q5: got=%s expected=%s\n\n' "$$got" "$$exp"; \
	fi
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: q5_test_fixed

all_test_fixed:
	rm -f results/clients/client_0/q1.csv results/clients/client_0/q2.csv results/clients/client_0/q3.csv results/clients/client_0/q5.txt
	cp ./scenarios/all/1.yaml docker-compose.yaml
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	@echo "Waiting for client_0 to finish..."
	@docker wait client_0
	-python3 scripts/compare_results.py q1
	-python3 scripts/compare_results.py q2
	-python3 scripts/compare_results.py q3
	@got=$$(tr -d '[:space:]' < results/clients/client_0/q5.txt 2>/dev/null); \
	exp=$$(tr -d '[:space:]' < results/fixed/q5.txt); \
	if [ "$$got" = "$$exp" ]; then \
		printf '\n\033[1;32mPASS\033[0m  q5 (count=%s)\n\n' "$$got"; \
	else \
		printf '\n\033[1;31mFAIL\033[0m  q5: got=%s expected=%s\n\n' "$$got" "$$exp"; \
	fi
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: all_test_fixed
