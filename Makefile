.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help up run down clean logs psql status test lint verify-schema install

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up:  ## Build and run the whole pipeline end-to-end (the main entry point)
	$(COMPOSE) up --build --abort-on-container-exit --exit-code-from pipeline

run:  ## Re-run the pipeline against the already-running database
	$(COMPOSE) run --rm pipeline python -m src.cli run

smoke:  ## Quick run with 10 images per class — for checking wiring, not data
	$(COMPOSE) run --rm pipeline python -m src.cli run --limit 10

status:  ## Show recent runs and current dataset composition
	$(COMPOSE) run --rm pipeline python -m src.cli status

psql:  ## Open a psql shell against the pipeline database
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-pipeline} -d $${POSTGRES_DB:-imagedb}

verify-schema:  ## Run the schema smoke test (12 assertions, rolls back)
	$(COMPOSE) exec -T postgres psql -U $${POSTGRES_USER:-pipeline} -d $${POSTGRES_DB:-imagedb} \
		-f - < sql/verify_schema.sql

logs:  ## Tail pipeline logs
	$(COMPOSE) logs -f pipeline

down:  ## Stop containers, keep data
	$(COMPOSE) down

clean:  ## Stop containers and delete the database volume and all outputs
	$(COMPOSE) down -v
	rm -rf data/raw data/images data/output
	@echo "cleaned. next 'make up' starts from a completely empty state."

install:  ## Install dependencies locally (for running tests outside Docker)
	pip install -r requirements-dev.txt

test:  ## Run the unit tests
	pytest -q tests/

lint:  ## Lint
	ruff check src/ tests/
