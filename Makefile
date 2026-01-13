.PHONY: help lint-md lint-md-fix lint test setup preflight

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

lint-md: ## Run markdown linter on all markdown files
	@echo "Running markdownlint..."
	@npx --yes markdownlint-cli "**/*.md" --config .markdownlint.json

lint-md-fix: ## Auto-fix markdown linting issues
	@echo "Fixing markdown linting issues..."
	@npx --yes markdownlint-cli "**/*.md" --config .markdownlint.json --fix

lint: lint-md ## Run all linters

test: ## Run tests
	@echo "Running tests..."
	@cd validation && poetry run pytest tests/ -v

setup: ## Install dependencies, start local services, and run preflight checks
	@bash scripts/dev_setup.sh

preflight: ## Run preflight checks without starting services
	@poetry run python scripts/preflight_check.py
