.PHONY: help install install-dev test test-unit test-integration test-e2e lint format type-check clean build docker-up docker-down docker-logs ingest-test-data

# Default target
help:
	@echo "Available targets:"
	@echo "  install          - Install package in development mode"
	@echo "  install-dev      - Install with development dependencies"
	@echo "  test             - Run all tests"
	@echo "  test-unit        - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-e2e         - Run end-to-end tests"
	@echo "  lint             - Run linting checks"
	@echo "  format           - Format code"
	@echo "  type-check       - Run type checking"
	@echo "  clean            - Clean build artifacts"
	@echo "  build            - Build package"
	@echo "  docker-up        - Start DataHub test environment"
	@echo "  docker-down      - Stop DataHub test environment"
	@echo "  docker-logs      - Show DataHub logs"
	@echo "  ingest-test-data - Ingest test data into DataHub"

# Installation
install:
	pip install -e .

install-dev:
	pip install -e ".[dev,test]"

# Testing
test: test-unit test-integration

test-unit:
	pytest tests/unit/ -v

test-integration: docker-up ingest-test-data
	@echo "Running integration tests..."
	DATAHUB_GMS_URL=http://localhost:8080 pytest tests/integration/ -v -m integration
	@$(MAKE) docker-down

test-e2e: docker-up ingest-test-data
	@echo "Running end-to-end tests..."
	DATAHUB_GMS_URL=http://localhost:8080 pytest tests/ -v -m "not slow"
	@$(MAKE) docker-down

# Code quality
lint:
	ruff check .
	ruff format --check .

format:
	ruff format .
	black .
	isort .

type-check:
	mypy src/datahub_cicd_client

# Build and clean
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

# Docker operations
docker-up:
	@echo "Starting DataHub test environment..."
	docker-compose -f docker-compose.test.yml up -d
	@echo "Waiting for DataHub to be ready..."
	@timeout 300 bash -c 'until curl -f http://localhost:8080/health >/dev/null 2>&1; do echo "Waiting for DataHub GMS..."; sleep 10; done'
	@timeout 300 bash -c 'until curl -f http://localhost:9002/admin >/dev/null 2>&1; do echo "Waiting for DataHub Frontend..."; sleep 10; done'
	@echo "DataHub is ready!"

docker-down:
	@echo "Stopping DataHub test environment..."
	docker-compose -f docker-compose.test.yml down -v

docker-logs:
	docker-compose -f docker-compose.test.yml logs -f

ingest-test-data:
	@echo "Ingesting test data..."
	docker-compose -f docker-compose.test.yml cp tests/scripts/ingest_test_data.py datahub-ingestion:/tmp/
	docker-compose -f docker-compose.test.yml exec -T datahub-ingestion python /tmp/ingest_test_data.py
	@echo "Waiting for data to be indexed..."
	@sleep 30

# Development helpers
dev-setup: install-dev
	pre-commit install

quick-test:
	pytest tests/unit/ -x -v

check-all: lint type-check test-unit
	@echo "All checks passed!"

# CI simulation
ci-test: clean install-dev check-all test-integration
	@echo "CI tests completed successfully!" 