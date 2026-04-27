.PHONY: up down build migrate seed logs test shell

up:
	docker-compose up -d

down:
	docker-compose down -v

build:
	docker-compose build

migrate:
	docker-compose run --rm api alembic upgrade head

seed:
	docker-compose run --rm api python -m app.seed

logs:
	docker-compose logs -f api celery_worker

kafka-logs:
	docker-compose logs -f kafka_consumer

beat-logs:
	docker-compose logs -f celery_beat

test:
	docker-compose run --rm api pytest tests/ -v

shell:
	docker-compose run --rm api python

reset:
	docker-compose down -v
	docker-compose up -d
	sleep 10
	$(MAKE) migrate
	$(MAKE) seed
