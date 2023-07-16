init:
	pip3 install -r requirements.txt
	pre-commit install

start:
	export PYTHONPATH=.
	export PYTHON_ENV=local

lint-fix:
	@echo
	@echo --- Lint Fix --- ;\
	pre-commit run --all-files
	@echo --- Lint Completed ---

test:
	@echo
	@echo --- Testing --- ;\
	export PYTHON_ENV=develop
	pytest
	@echo --- Testing Completed ---
