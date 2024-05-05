init:
	poetry install

lint-fix:
	@echo
	@echo --- Lint Fix ---
	pre-commit run --all-files
	@echo --- Lint Completed ---

test:
	@echo
	@echo --- Testing ---
	pytest ${TEST_SUBDIR}
	@echo --- Testing Completed ---

# Prefect
prefect-local:
	prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
	prefect server start

# DOCKER
build:
	docker-compose up --build

up:
	docker-compose up -d
