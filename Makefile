.PHONY: help run clean lint

help:
	@echo "Available commands:"
	@echo "  make run              - Run the Streamlit application"
	@echo "  make clean            - Clean up temporary files"
	@echo "  make lint             - Run pre-commit hooks to lint and format code"

run:
	@echo "Running Streamlit application..."
	uv run streamlit run onboarding/streamlit_app.py

lint:
	@echo "Running pre-commit hooks..."
	pre-commit run --all-files
	@echo "Linting complete"

clean:
	@echo "Cleaning up temporary files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Cleanup complete"
