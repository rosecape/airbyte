name = airbyte/source-lightspeed-retail
tag = dev

build:
	docker build --platform linux/amd64 . -t $(name):$(tag)

spec:
	python3 main.py spec

check:
	python3 main.py check --config secrets/config.json

discover:
	python3 main.py discover --config secrets/config.json

read:
	 python3 main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json

read-debug:
	 python3 main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json --debug

read-state:
	python3 main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json --state sample_files/state.json --debug