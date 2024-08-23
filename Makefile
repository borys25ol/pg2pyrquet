ve:
	python3 -m venv .ve; \
	. .ve/bin/activate; \
	pip install -r requirements.txt; \

install_hooks:
	pip install -r requirements-ci.txt; \
	pre-commit install; \

run_hooks:
	pre-commit run --all-files

check_style:
	flake8 pg2pyrquet && isort pg2pyrquet --diff && black pg2pyrquet --check

lint:
	flake8 pg2pyrquet && isort pg2pyrquet && black pg2pyrquet

types:
	mypy --namespace-packages -p "pg2pyrquet" --config-file setup.cfg
