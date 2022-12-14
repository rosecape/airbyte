name = airbyte/source-booker
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
	 python3 main.py read --config secrets/config.json --catalog sample_files/catalog.json


read-state:
	docker run --rm -v $(current_dir)/secrets:/secrets -v $(current_dir)/sample_files:/sample_files $(name):$(tag) read --config /secrets/config.json --catalog /sample_files/catalog.json --state /sample_files/state.json