# Generic Makefile for use in all ConSci repos

PYTHON=python3.10
VENV=.venv
BIN=$(VENV)/bin
ACTIVATE_VENV=$(VENV_DIR)/bin/activate
REQUIREMENTS_FILE=requirements.txt

# Checks if python is installed
.PHONY: check.python
check.python:
ifndef PYTHON
	$(error python3 is not installed or in PATH);
endif
	@exit 0;

# ## Setup
# Setup virtual environment
setup.venv: check.python
	@test -d $(VENV) || \
    ($(PYTHON) -m venv $(VENV););

setup.PYTHONPATH: check.python
	export PYTHONPATH=$$PYTHONPATH:"$(shell pwd)/algo_features";

# Setup dependencies from requirements.txt
setup.deps: setup.venv $(REQUIREMENTS_FILE)
	$(BIN)/pip install --upgrade pip && \
	$(BIN)/pip install -r $(REQUIREMENTS_FILE)

# Setup tooling
setup.tools: setup.venv
	$(BIN)/pip install pytest ruff=0.0.291 pep8-naming mypy==1.2.0 coverage pytest-cov pytest-mock pytest-mock-generator

.PHONY: setup.pre-commit
setup.pre-commit: setup.venv
	$(BIN)/pip install pre-commit && \
	$(BIN)/pre-commit install;

.PHONY: setup
setup: setup.deps setup.tools setup.pre-commit

# Install Airflow and Kubernetes packages for local dev
setup.airflow-deps: setup.venv
	$(BIN)/pip install apache-airflow kubernetes


# Run ruff linting
.PHONY: lint.ruff
lint.ruff: setup.deps setup.tools
	$(BIN)/ruff check algo_features tests;

# Run mypy linting
.PHONY: lint.mypy
lint.mypy: setup.deps setup.tools
	$(BIN)/mypy algo_features

# Run all linters
.PHONY: lint
lint: lint.ruff lint.mypy

# Run unit tests
.PHONY: test
test: setup.deps setup.tools setup.PYTHONPATH
	@export PYTHONPATH=$$PYTHONPATH:"$(shell pwd)/algo_features"; \
	$(BIN)/pytest --cov=algo_features -m "not integration and not network"

test.integration: setup.deps setup.tools setup.PYTHONPATH
	@export PYTHONPATH=$$PYTHONPATH:"$(shell pwd)/algo_features"; \
	$(BIN)/pytest --cov=algo_features -m "integration and not network"

test.network: setup.deps setup.tools setup.PYTHONPATH
	@export PYTHONPATH=$$PYTHONPATH:"$(shell pwd)/algo_features"; \
	$(BIN)/pytest --cov=algo_features -m "network"

test.all: setup.deps setup.tools setup.PYTHONPATH
	@export PYTHONPATH=$$PYTHONPATH:"$(shell pwd)/algo_features"; \
	$(BIN)/pytest --cov=algo_features

# Install package with pip and update requirements file
.PHONY: install
install: setup.deps setup.tools
	$(BIN)/pip install $(PACKAGES)
	$(BIN)/pip freeze > $(REQUIREMENTS_FILE)

test.check_feature_store_yaml: setup.deps setup.PYTHONPATH
	@export PYTHONPATH=$$PYTHONPATH:"$(shell pwd)/algo_features";\
	$(BIN)/python algo_features/scripts/check_for_deletions.py

dag:
	@export PYTHONPATH=$$PYTHONPATH:"$(shell pwd)/pipelines";\
	$(BIN)/python pipelines/builders/entrypoint.py --config_path=algo_features/configs/user_features.yaml --builder=airflow
