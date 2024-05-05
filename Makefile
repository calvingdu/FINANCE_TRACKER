init:
	poetry install
	pre-commit install

lint-fix:
	@echo
	@echo --- Lint Fix --- ;\
	pre-commit run --all-files
	@echo --- Lint Completed ---

test:
	@echo
	@echo --- Testing --- ;\

	pytest ${TEST_SUBDIR}
	@echo --- Testing Completed ---

# DOCKER
build:
	docker-compose up --build

up:
	docker-compose up -d
