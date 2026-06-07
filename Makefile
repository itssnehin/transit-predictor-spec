# Transit Predictor — Developer commands
#
# This Makefile is a target spec, not the working file. As components are built,
# the targets become real. Each target represents a developer workflow that the
# project must support.

.PHONY: help bootstrap up down restart logs ps test test-unit test-integration \
        lint typecheck format build push deploy demo clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

## Local stack

up:  ## Start the local Docker Compose stack
	docker compose up -d

down:  ## Stop and remove containers (keeps volumes)
	docker compose down

restart:  ## Restart the stack
	docker compose restart

logs:  ## Tail logs from all services
	docker compose logs -f

ps:  ## Show running services
	docker compose ps

bootstrap:  ## Initialise buckets, topics, schemas, etc. (run once after first `make up`)
	./scripts/bootstrap.sh

clean:  ## Stop stack and remove volumes (DESTRUCTIVE)
	docker compose down -v

## Code quality

lint:  ## Lint with ruff
	uv run ruff check .

format:  ## Format with ruff
	uv run ruff format .

typecheck:  ## Type-check with mypy
	uv run mypy services/ dbt/

## Tests

test: test-unit test-integration  ## Run all tests

test-unit:  ## Run fast unit tests (no Docker required)
	uv run pytest -m "not integration" -v

test-integration:  ## Run integration tests (requires Docker)
	uv run pytest -m "integration" -v

## Build

build:  ## Build all service Docker images
	docker compose build

## Cloud (Terraform)

tf-plan:  ## Show pending infrastructure changes (dev env)
	cd infra/terraform/envs/dev && terraform plan

tf-apply:  ## Apply infrastructure changes (dev env) -- REQUIRES CONFIRMATION
	cd infra/terraform/envs/dev && terraform apply

tf-destroy:  ## Tear down all dev infrastructure -- DESTRUCTIVE
	cd infra/terraform/envs/dev && terraform destroy

## End-to-end demo

demo:  ## Bring up local stack, train a model, run example predictions
	./scripts/demo.sh

## CI emulation

ci:  ## Run the same checks GitHub Actions runs
	$(MAKE) lint
	$(MAKE) typecheck
	$(MAKE) test-unit
	$(MAKE) build

