# ═══════════════════════════════════════════════════════════════════════
# MRCA AI Tariff — Makefile
# Shorthand commands for Docker Compose, local dev, testing & cleanup
# ═══════════════════════════════════════════════════════════════════════

.PHONY: help build up down restart logs \
        dev dev-backend dev-frontend \
        test test-engine test-api test-verify \
        pipeline lint clean nuke

# ── Defaults ──────────────────────────────────────────────────────────
COMPOSE  := docker compose
PYTHON   := python3
PYTEST   := $(PYTHON) -m pytest
FRONTEND := frontend

# ═══════════════════════════════════════════════════════════════════════
# Help
# ═══════════════════════════════════════════════════════════════════════
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ═══════════════════════════════════════════════════════════════════════
# Docker Compose — containerised stack
# ═══════════════════════════════════════════════════════════════════════
build: ## Build backend + frontend Docker images
	$(COMPOSE) build backend frontend

up: ## Start backend + frontend containers (ports 8000, 3000)
	$(COMPOSE) --profile app up -d

down: ## Stop all containers
	$(COMPOSE) --profile app --profile pipeline down

restart: down up ## Restart containers

logs: ## Tail backend logs
	$(COMPOSE) --profile app logs -f backend

shell: ## Open a shell inside the backend container
	$(COMPOSE) --profile app exec backend bash

# ═══════════════════════════════════════════════════════════════════════
# Local development (no Docker)
# ═══════════════════════════════════════════════════════════════════════
dev: ## Start backend + frontend locally (parallel)
	@echo "Starting backend on :8000 and frontend on :3000 …"
	@trap 'kill 0' INT; \
		$(PYTHON) -m uvicorn backend.main:app --reload --port 8000 & \
		cd $(FRONTEND) && npm run dev & \
		wait

dev-backend: ## Start backend only (uvicorn --reload)
	$(PYTHON) -m uvicorn backend.main:app --reload --port 8000

dev-frontend: ## Start frontend only (Next.js dev server)
	cd $(FRONTEND) && npm run dev

frontend-build: ## Production build of the Next.js frontend
	cd $(FRONTEND) && npm run build

frontend-install: ## Install frontend npm dependencies
	cd $(FRONTEND) && npm install

# ═══════════════════════════════════════════════════════════════════════
# Testing
# ═══════════════════════════════════════════════════════════════════════
test: ## Run ALL tests (engine + API)
	$(PYTEST) tests/test_engine.py tests/test_api.py -v --tb=short

test-engine: ## Run tariff engine unit tests only
	$(PYTEST) tests/test_engine.py -v

test-api: ## Run API integration tests only
	$(PYTEST) tests/test_api.py -v

test-docker: ## Run tests inside Docker container
	$(COMPOSE) --profile app run --rm backend $(PYTEST) tests/ -v --tb=short

# ═══════════════════════════════════════════════════════════════════════
# Pipeline
# ═══════════════════════════════════════════════════════════════════════
pipeline: ## Run ingestion pipeline
	$(COMPOSE) --profile pipeline run --rm pipeline

# ═══════════════════════════════════════════════════════════════════════
# Lint
# ═══════════════════════════════════════════════════════════════════════
lint: ## Lint backend (ruff) + frontend (eslint)
	-$(PYTHON) -m ruff check backend/ tests/
	-cd $(FRONTEND) && npm run lint

# ═══════════════════════════════════════════════════════════════════════
# Cleanup
# ═══════════════════════════════════════════════════════════════════════
clean: ## Remove Python caches, test artifacts, frontend build
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache htmlcov .coverage coverage.xml
	rm -rf $(FRONTEND)/.next

nuke: down clean ## Stop containers + remove images, volumes & caches
	$(COMPOSE) --profile app --profile pipeline down --rmi local -v
	rm -rf $(FRONTEND)/node_modules
