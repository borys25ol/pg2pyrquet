ve:
	python3 -m venv .ve; \
	. .ve/bin/activate; \
	pip install -r requirements.txt; \

install_hooks:
	pip install -r requirements-ci.txt; \
	pre-commit install; \

run_hooks:
	pre-commit run --all-files

test:
	python -m pytest -v -m "not integration" ./tests

test-integration:
	python -m pytest -v -m integration ./tests

test-cov:
	python -m pytest --cov=./pg2pyrquet --cov-report term-missing -m "not integration" ./tests

check_style:
	flake8 pg2pyrquet tests && isort pg2pyrquet tests --check-only --diff && black pg2pyrquet tests --check

lint:
	flake8 pg2pyrquet tests && isort pg2pyrquet tests && black pg2pyrquet tests

types:
	mypy --namespace-packages -p "pg2pyrquet" --config-file setup.cfg
