.PHONY: up down test migrate format get-models

ifneq (,$(wildcard ./.env))
    include .env
    export
endif

VENV=./.venv
PYBINBOT_REPO ?= ../pybinbot

ifneq (,$(wildcard $(PYBINBOT_REPO)/pyproject.toml))
PYBINBOT_PYTHONPATH := PYTHONPATH=$(abspath $(PYBINBOT_REPO))
PYBINBOT_MYPYPATH := MYPYPATH=$(abspath $(PYBINBOT_REPO))
else
PYBINBOT_PYTHONPATH :=
PYBINBOT_MYPYPATH :=
endif

cmd-exists-%:
	@hash $(*) > /dev/null 2>&1 || \
		(echo "ERROR: '$(*)' must be installed and available on your PATH."; exit 1)

up:  ## Run Docker Compose services
	docker compose up --pull always -d --build 

down:  ## Shutdown Docker Compose services
	docker compose down --volumes --remove-orphans

test:  ## Run tests
	$(PYBINBOT_PYTHONPATH) uv run pytest -v --tb=short --disable-warnings --maxfail=1

migrate:  ## Apply latest alembic migrations
	uv run alembic upgrade head
	uv run alembic revision --autogenerate -m "$(message)" --head head

format: 
	@uv run ruff format .
	@uv run ruff check .  --fix
	@$(PYBINBOT_MYPYPATH) uv run mypy .

upgrade-pybinbot:
	source ./.venv/bin/activate;
	@uv cache clean
	@uv remove pybinbot
	@uv add pybinbot --upgrade
	@uv sync --extra dev
