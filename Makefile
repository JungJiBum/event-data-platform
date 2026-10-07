CORE_COMPOSE = docker compose
PIPELINE_COMPOSE = docker compose --env-file .env -f pipeline/docker-compose.yml
MONITORING_COMPOSE = docker compose --env-file .env -f monitoring/docker-compose.yml

.PHONY: up down build ps logs

up:
	$(CORE_COMPOSE) up -d
	$(PIPELINE_COMPOSE) up -d
	$(MONITORING_COMPOSE) up -d

build:
	$(CORE_COMPOSE) build
	$(PIPELINE_COMPOSE) build
	$(MONITORING_COMPOSE) build

down:
	$(MONITORING_COMPOSE) down
	$(PIPELINE_COMPOSE) down
	$(CORE_COMPOSE) down

ps:
	$(CORE_COMPOSE) ps
	$(PIPELINE_COMPOSE) ps
	$(MONITORING_COMPOSE) ps

logs:
	$(CORE_COMPOSE) logs -f