.PHONY: sync test run-gmaps

sync:
	uv sync
	uv run python scripts/patch_nodriver.py

test:
	uv run python -m pytest tests/ -v

run-gmaps:
	uv run python main.py --config config/google_maps.yaml --query "$(QUERY)"
