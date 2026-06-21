SHELL := /bin/bash
PWD := $(shell pwd)

up:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
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
	@echo "3) Pruebas con datos fixed: 1 cliente y N workers,"
	@echo "2) Pruebas con datos fixed: 3 cliente y N workers,"
	@read -p "Selecciona uno: " option;	\
	cp ./scenarios/all/$${option}.yaml docker-compose.yaml
.PHONY: q5_switch

#
# all_test_fixed:
# 	rm -f results/clients/client_0/q1.csv results/clients/client_0/q2.csv results/clients/client_0/q3.csv results/clients/client_0/q5.csv
# 	cp ./scenarios/all/1.yaml docker-compose.yaml
# 	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
# 	@echo "Waiting for client_0 to finish..."
# 	@docker wait client_0
# 	-python3 scripts/compare_results.py q1
# 	-python3 scripts/compare_results.py q2
# 	-python3 scripts/compare_results.py q3
# 	-python3 scripts/compare_results.py q5
# 	docker compose -f docker-compose.yaml stop -t 1
# 	docker compose -f docker-compose.yaml down
# .PHONY: all_test_fixed
#
# all_multi_test_fixed:
# 	rm -f results/clients/client_*/q1.csv results/clients/client_*/q2.csv results/clients/client_*/q3.csv results/clients/client_*/q5.csv
# 	cp ./scenarios/all/2.yaml docker-compose.yaml
# 	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
# 	@echo "Waiting for all clients to finish..."
# 	@docker wait client_0 client_1 client_2
# 	-python3 scripts/compare_results.py q1
# 	-python3 scripts/compare_results.py q2
# 	-python3 scripts/compare_results.py q3
# 	-python3 scripts/compare_results.py q5
# 	docker compose -f docker-compose.yaml stop -t 1
# 	docker compose -f docker-compose.yaml down
# .PHONY: all_multi_test_fixed
#
# all_scaled_test_fixed:
# 	rm -f results/clients/client_0/q1.csv results/clients/client_0/q2.csv results/clients/client_0/q3.csv results/clients/client_0/q5.csv
# 	cp ./scenarios/all/3.yaml docker-compose.yaml
# 	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
# 	@echo "Waiting for client_0 to finish..."
# 	@docker wait client_0
# 	-python3 scripts/compare_results.py q1
# 	-python3 scripts/compare_results.py q2
# 	-python3 scripts/compare_results.py q3
# 	-python3 scripts/compare_results.py q5
# 	docker compose -f docker-compose.yaml stop -t 1
# 	docker compose -f docker-compose.yaml down
# .PHONY: all_scaled_test_fixed
#
# all_scaled_multi_test_fixed:
# 	rm -f results/clients/client_*/q1.csv results/clients/client_*/q2.csv results/clients/client_*/q3.csv results/clients/client_*/q5.csv
# 	cp ./scenarios/all/4.yaml docker-compose.yaml
# 	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
# 	@echo "Waiting for all clients to finish..."
# 	@docker wait client_0 client_1 client_2
# 	-python3 scripts/compare_results.py q1
# 	-python3 scripts/compare_results.py q2
# 	-python3 scripts/compare_results.py q3
# 	-python3 scripts/compare_results.py q5
# 	docker compose -f docker-compose.yaml stop -t 1
# 	docker compose -f docker-compose.yaml down
# .PHONY: all_scaled_multi_test_fixed
#
# # ---- Monitoreo de logs (sin ruido de pika/rabbit) ----
# LOG_NOISE := pika|AMQP|Streaming transport|Socket connected|Created channel
# # FOLLOW=1 (default) sigue en vivo; FOLLOW=0 vuelca lo que hay y termina.
# FOLLOW ?= 1
# _FOLLOW := $(if $(filter 0,$(FOLLOW)),,-f)
#
# # Logs de UN container (worker o cliente), sin ruido.
# #   make log SVC=q3_group_1            (en vivo)
# #   make log SVC=gateway FOLLOW=0      (vuelca y sale)
# log:
# 	@test -n "$(SVC)" || { echo 'Falta SVC. Ej: make log SVC=q3_group_1'; exit 2; }
# 	@docker logs $(_FOLLOW) --tail 200 $(SVC) 2>&1 | grep --line-buffered -avE '$(LOG_NOISE)'
# .PHONY: log
#
# # Logs de TODOS los containers cuyo nombre contenga PREFIX, mergeados y sin ruido.
# #   make logs_stage PREFIX=q3_group            (en vivo)
# #   make logs_stage PREFIX=client FOLLOW=0     (vuelca y sale)
# logs_stage:
# 	@test -n "$(PREFIX)" || { echo 'Falta PREFIX. Ej: make logs_stage PREFIX=q3_group | client | q5'; exit 2; }
# 	@names=$$(docker ps -a --filter "name=$(PREFIX)" --format '{{.Names}}' | sort); \
# 	test -n "$$names" || { echo "No hay containers que matcheen '$(PREFIX)'"; exit 1; }; \
# 	echo "==> $$names"; \
# 	docker compose -f docker-compose.yaml logs $(_FOLLOW) --tail 50 $$names 2>&1 \
# 		| grep --line-buffered -avE '$(LOG_NOISE)'
# .PHONY: logs_stage
# # Corre un scenario con un dataset de data/datasets/ (por nombre).
# #   make dataset_run SCENARIO=all/4 DATASET=HI-Small
# # Toma {DATASET}_Trans.csv y {DATASET}_accounts.csv para cada cliente.
# dataset_run:
# 	@test -n "$(SCENARIO)" || { echo 'Falta SCENARIO. Ej: make dataset_run SCENARIO=all/4 DATASET=HI-Small'; exit 2; }
# 	@test -n "$(DATASET)"  || { echo 'Falta DATASET (ej: HI-Small, LI-Small, HI-Medium...)'; exit 2; }
# 	@test -f scenarios/$(SCENARIO).yaml || { echo "No existe scenarios/$(SCENARIO).yaml"; exit 2; }
# 	@test -f data/datasets/$(DATASET)_Trans.csv || { echo "No existe data/datasets/$(DATASET)_Trans.csv"; exit 2; }
# 	@test -f data/datasets/$(DATASET)_accounts.csv || { echo "No existe data/datasets/$(DATASET)_accounts.csv"; exit 2; }
# 	rm -f results/clients/client_*/q*.csv
# 	cp ./scenarios/$(SCENARIO).yaml docker-compose.yaml
# 	python3 scripts/gen_dataset_override.py docker-compose.yaml $(DATASET) > docker-compose.dataset.yaml
# 	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml -f docker-compose.dataset.yaml up --build --remove-orphans -d
# 	@echo "==> $(SCENARIO) con dataset $(DATASET). Esperando a los clientes..."
# 	@clients=$$(docker ps -a --filter "name=client" --format '{{.Names}}'); \
# 	docker wait $$clients > /dev/null; \
# 	echo ""; echo "==> Tiempos (StartedAt -> FinishedAt, incluye el sleep inicial del cliente):"; \
# 	for c in $$clients; do \
# 		s=$$(docker inspect -f '{{.State.StartedAt}}' $$c); \
# 		e=$$(docker inspect -f '{{.State.FinishedAt}}' $$c); \
# 		printf '    %s: %s s\n' "$$c" "$$(( $$(date -d "$$e" +%s) - $$(date -d "$$s" +%s) ))"; \
# 	done
# 	@echo ""
# 	@echo "==> Resultados en results/clients/. La pila sigue arriba."
# 	@echo "    bajar: docker compose -f docker-compose.yaml -f docker-compose.dataset.yaml down"
# .PHONY: dataset_run
