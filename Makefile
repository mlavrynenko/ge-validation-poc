.PHONY: lint format

format:
	@echo "🛠️ Formatting Python..."
	ruff check . --fix
	@echo "✅ Formatting complete"

lint:
	@echo "🔍 Running linters..."
	yamllint .
	ruff check .
	@echo "✅ All lint checks passed"
