init:
	pip3 install -r requirements.txt
	pre-commit install

lint-fix:
	@echo
	@echo --- Lint Fix --- ;\
	pre-commit run --all-files
	@echo --- Lint Completed ---
