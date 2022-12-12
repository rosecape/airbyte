name = airbyte/source-lightspeed-retail
tag = dev
current_dir = $(shell pwd)

build:
	docker build . -t $(name):$(tag)

spec:
	docker run --rm $(name):$(tag) spec

check:
	docker run --rm -v $(current_dir)/secrets:/secrets $(name):$(tag) check --config /secrets/config.json

discover:
	docker run --rm -v $(current_dir)/secrets:/secrets $(name):$(tag) discover --config /secrets/config.json

read:
	docker run --rm -v $(current_dir)/secrets:/secrets -v $(current_dir)/integration_tests:/integration_tests $(name):$(tag) read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json

test:
	./acceptance-test-docker.sh