UV ?= uv

.PHONY: install lint format format-check typecheck security test build run all
install:
	$(UV) sync --locked
format:
	$(UV) run --locked ruff format kairos_text tests
format-check:
	$(UV) run --locked ruff format --check kairos_text tests
lint:
	$(UV) run --locked ruff check kairos_text tests
typecheck:
	$(UV) run --locked mypy kairos_text
security:
	$(UV) run --locked bandit -q -r kairos_text -x tests
test:
	$(UV) run --locked pytest -q --tb=short
build:
	$(UV) build --no-sources
run:
	$(UV) run --locked python -m kairos_text
all: lint format-check typecheck security test build
