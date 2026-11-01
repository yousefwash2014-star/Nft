.PHONY: build up down logs test shell compile
build:
	docker compose build
up:
	docker compose up -d

down:
	docker compose down
logs:
	docker compose logs -f mint-bot
test:
	docker compose --profile test run --rm test-runner
shell:
	docker compose exec mint-bot sh
compile:
	python -m py_compile *.py
