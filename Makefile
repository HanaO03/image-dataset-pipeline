.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help up run smoke down clean logs psql status test test-local lint verify-schema install

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
	rm -rf data/images data/output
	@echo "cleaned. next 'make up' starts from a completely empty state."

test:  ## Run the full suite in Docker, integration tests included (needs no local Python)
	$(COMPOSE) --profile test build tests
	$(COMPOSE) --profile test run --rm tests

install:  ## Install dependencies locally (only needed for `make test-local`)
	pip install -r requirements-dev.txt

test-local:  ## Run the suite on the host — requires Python 3.12+ and `make install`
	pytest -q tests/

lint:  ## Lint
	ruff check src/ tests/
