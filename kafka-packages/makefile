.PHONY: install run test py-source

install:
	poetry install

py-source:
	. .venv/bin/activate && \
	export PATH=".venv/bin::PATH" && \
	exec "$$SHELL"

build:
	poetry build

run:
	cd src && ENV="local" python3 app.py

ruff-check:
	poetry run ruff check --fix . --config .ruff.toml

ruff-format:
	poetry run ruff format . --config .ruff.toml

test:
	pytest test/

standards:
	@make ruff-check
	@echo "Code standards check complete."
	@make ruff-format
	@echo "Code formatting complete."