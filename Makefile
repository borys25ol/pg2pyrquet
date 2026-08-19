ve:
	uv sync

install_hooks:
	uv run pre-commit install

run_hooks:
	uv run pre-commit run --all-files

test:
	uv run pytest -v -m "not integration" ./tests

test-integration:
	uv run pytest -v -m integration ./tests

test-cov:
	uv run pytest --cov=./pg2pyrquet --cov-report term-missing -m "not integration" ./tests

check_style:
	uv run ruff check pg2pyrquet tests && uv run ruff format pg2pyrquet tests --check

lint:
	uv run ruff check pg2pyrquet tests --fix && uv run ruff format pg2pyrquet tests

types:
	uv run mypy --namespace-packages -p "pg2pyrquet"
