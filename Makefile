.PHONY: tests durability docs

tests:
	uv run --all-extras pytest tests $(ARGS)

durability:
	@set -eu; \
	trap 'docker compose -f docker-compose.durability.yml down -v' EXIT; \
	docker compose -f docker-compose.durability.yml up -d --wait; \
	uv run --all-extras pytest tests/durability --run-durability -n 0 $(ARGS)

docs:
	uv run --extra web --extra cron --group screenshots python -m examples.screenshots
	uv run --all-extras $(MAKE) -C docs clean html
