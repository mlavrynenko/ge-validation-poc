# Makefile

.PHONY: lint lint-yaml lint-python

lint: lint-yaml lint-python
	@echo "✅ All lint checks passed"

lint-yaml:
	@echo "🔍 Linting YAML files..."
	yamllint .

lint-python:
	@echo "🐍 Linting Python code..."
	ruff check . --fix
