.PHONY: install lint test format run
install:
	pip install -e ".[dev]"
format:
	ruff format kairos_text tests
lint:
	ruff check kairos_text tests
test:
	pytest -q
run:
	python -m kairos_text
