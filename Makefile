.PHONY: lint format

format:
	@echo "🛠️ Formatting Python..."
	ruff check . --fix
	@echo "✅ Formatting complete"

lint:
	@echo "🔍 Running linters..."
	yamllint -f parsable .
	ruff check .
	@echo "✅ All lint checks passed"
