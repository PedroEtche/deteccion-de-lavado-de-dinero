# --- Chaos testing del sistema de deteccion de fallas (heartbeat) ------------
HB_COMPOSE := docker-compose.yaml

chaos_up:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f $(HB_COMPOSE) up --build --remove-orphans --detach
	@echo "Cluster arriba. Esperando a que los nodos se vean..."
	@sleep 5
	docker compose -f $(HB_COMPOSE) ps
.PHONY: chaos_up

chaos_run:
	python3 chaos.py
.PHONY: chaos_run

chaos_down:
	docker compose -f $(HB_COMPOSE) stop -t 1
	docker compose -f $(HB_COMPOSE) down
.PHONY: chaos_down



