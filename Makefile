# ModelOps Bundles Development Makefile
#
# This Makefile provides convenient targets for local development,
# testing, and Docker service management.

.DEFAULT_GOAL := help
.PHONY: help up down ps logs test-fast test-real test clean shell-azurite shell-registry

help: ## Show this help message
	@echo 'ModelOps Bundles Development Commands'
	@echo '===================================='
	@echo ''
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ''
	@echo 'Examples:'
	@echo '  make up          # Start development services'
	@echo '  make test        # Run all tests'
	@echo '  make clean       # Clean everything'

up: ## Start development services (Azurite, OCI Registry, Registry UI)
	@echo "🚀 Starting development services..."
	docker-compose -f dev/docker-compose.yml up -d
	@echo "✅ Services started:"
	@echo "   🟦 Azurite (Azure):     http://localhost:10000"
	@echo "   📦 OCI Registry:        http://localhost:5555"
	@echo "   🖥️  Registry UI:         http://localhost:8080"
	@echo ""
	@echo "💡 Next steps:"
	@echo "   make test        # Run all tests"
	@echo "   make ps          # Check service status"

down: ## Stop development services
	@echo "🛑 Stopping development services..."
	docker-compose -f dev/docker-compose.yml down
	@echo "✅ Services stopped"

ps: ## Show development service status
	@echo "📋 Service Status:"
	docker-compose -f dev/docker-compose.yml ps

logs: ## Show logs from all services
	docker-compose -f dev/docker-compose.yml logs -f

logs-azurite: ## Show Azurite logs only
	docker-compose -f dev/docker-compose.yml logs -f azurite

logs-registry: ## Show OCI Registry logs only
	docker-compose -f dev/docker-compose.yml logs -f registry

test-fast: ## Run fast CLI tests with FakeProvider (no Docker required)
	@echo "🧪 Running fast CLI tests..."
	bash dev/test-cli-fake.sh

test-real: up ## Run real storage integration tests (requires Docker services)
	@echo "🔧 Running real storage integration tests..."
	@echo "   (This will start services automatically)"
	@source dev/dev.env && bash dev/test-storage-real.sh

test: test-fast test-real ## Run all test suites (fast + real)
	@echo "🎉 All test suites completed!"

unit: ## Run Python unit tests
	@echo "🐍 Running Python unit tests..."
	uv run python -m pytest tests/ -v

lint: ## Run code linting (if available)
	@echo "🔍 Running linting..."
	@if command -v uv >/dev/null 2>&1; then \
		if uv run python -c "import ruff" 2>/dev/null; then \
			uv run python -m ruff check src/; \
		else \
			echo "⚠️  ruff not installed, skipping lint"; \
		fi; \
	else \
		echo "⚠️  uv not available, skipping lint"; \
	fi

typecheck: ## Run type checking (if available)
	@echo "🔍 Running type checking..."
	@if command -v uv >/dev/null 2>&1; then \
		if uv run python -c "import mypy" 2>/dev/null; then \
			uv run python -m mypy src/; \
		else \
			echo "⚠️  mypy not installed, skipping typecheck"; \
		fi; \
	else \
		echo "⚠️  uv not available, skipping typecheck"; \
	fi

clean: down ## Clean up everything (stop services, remove volumes, temp files)
	@echo "🧹 Cleaning up development environment..."
	docker-compose -f dev/docker-compose.yml down -v
	@echo "🗑️  Removing temporary test files..."
	rm -rf /tmp/modelops-test-* /tmp/modelops-real-test-* 2>/dev/null || true
	@echo "✅ Cleanup complete"

reset: clean up ## Full reset: clean everything and restart services
	@echo "🔄 Full reset complete - services restarted"

shell-azurite: ## Open shell in Azurite container for debugging
	docker exec -it modelops-bundles-azurite /bin/bash

shell-registry: ## Open shell in OCI Registry container for debugging
	docker exec -it modelops-bundles-registry /bin/sh

# Aliases for common operations
start: up ## Alias for 'up'
stop: down ## Alias for 'down'
status: ps ## Alias for 'ps'