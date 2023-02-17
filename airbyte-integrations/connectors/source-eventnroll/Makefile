name = airbyte/source-eventnroll
tag = dev

build:
	docker build --platform linux/amd64 . -t $(name):$(tag)

spec:
	python3 main.py spec

check:
	python3 main.py check --config secrets/config.json --debug

discover:
	python3 main.py discover --config secrets/config.json

unit-test:
	python -m pytest -s unit_tests

integration-test:
	python -m pytest -s integration_tests

read:
	 python3 main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json

read-debug:
	 python3 main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json --debug