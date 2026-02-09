# OHLC Handler – local and Docker workflows
# Copy .env_example to .env and set DB_* and BINANCE_API_URL before running.

.PHONY: up down build logs postgres-up init-db backfill run setup psql help

# Default: full stack (postgres + ohlc-handler). Rebuilds image so code changes are picked up.
up:
	docker-compose up -d --build

down:
	docker-compose down

build:
	docker-compose build

logs:
	docker-compose logs -f

# Start only Postgres (for local dev: then use make run and make backfill with .env DB_HOST=localhost)
postgres-up:
	docker-compose up -d postgres
	@echo "Wait a few seconds then: make init-db"

# Create tables. Requires Postgres running (make up or make postgres-up).
init-db:
	docker-compose run --rm ohlc-handler python db/init_db.py

# Initial backfill: fetch OHLC and compute indicators for all tickers/timeframes. Run after init-db.
backfill:
	docker-compose run --rm ohlc-handler python processor.py

# Run API locally (requires Postgres up and .env with DB_HOST=localhost)
run:
	uvicorn api:app --reload --host 0.0.0.0 --port 8000

# One-shot: build, start postgres, create tables, then run backfill (for first-time setup)
setup: build postgres-up
	@sleep 3
	$(MAKE) init-db
	$(MAKE) backfill

# Connect to Postgres (uses DB_USER/DB_NAME from .env if present)
psql:
	@set -a; [ -f .env ] && . ./.env; set +a; \
	docker-compose exec postgres psql -U "$${DB_USER:-postgres}" -d "$${DB_NAME:-ohlc_data}"

help:
	@echo "OHLC Handler targets:"
	@echo "  make up         - Start postgres + ohlc-handler (Docker)"
	@echo "  make down       - Stop stack"
	@echo "  make build      - Build ohlc-handler image"
	@echo "  make logs       - Follow logs"
	@echo "  make postgres-up - Start only Postgres (for local app dev)"
	@echo "  make init-db    - Create DB tables (run after postgres is up)"
	@echo "  make backfill   - Initial/full backfill (OHLC + indicators)"
	@echo "  make run        - Run API locally (uvicorn, .env DB_HOST=localhost)"
	@echo "  make setup      - postgres-up + init-db + backfill"
	@echo "  make psql       - Open psql in postgres container"
