# OHLC Handler. Copy .env_example to .env and set DB_*, BINANCE_API_URL.

.PHONY: build up down logs recreate run postgres-up init-db backfill setup psql clean

# Docker (same targets as alerts-service)
build:
	docker compose build

up:
	docker compose up -d

recreate:
	docker compose up -d --force-recreate

down:
	docker compose down

logs:
	docker compose logs -f

# OHLC-specific
postgres-up:
	docker compose up -d postgres
	@echo "Wait a few seconds then: make init-db"

init-db:
	docker compose run --rm ohlc-handler python db/init_db.py

backfill:
	docker compose run --rm ohlc-handler python processor.py

run:
	uvicorn api:app --reload --host 0.0.0.0 --port 8000

setup: build postgres-up
	@sleep 3
	$(MAKE) init-db
	$(MAKE) backfill

psql:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	docker compose exec postgres psql -U "$${DB_USER:-postgres}" -d "$${DB_NAME:-ohlc_data}"

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
