GO_DIR := go
GO_BIN := $(GO_DIR)/bin/emby-range-cache-proxy
CONFIG ?= config.example.json
IMAGE ?= emby-range-cache-proxy:local

.PHONY: test test-python test-go vet race build check-config docker-build clean

test: test-python test-go

test-python:
	python3 -m pytest -q

test-go:
	cd $(GO_DIR) && go test ./...

vet:
	cd $(GO_DIR) && go vet ./...

race:
	cd $(GO_DIR) && go test -race ./...

build:
	cd $(GO_DIR) && go build -o bin/emby-range-cache-proxy ./cmd/emby-range-cache-proxy

check-config: build
	./$(GO_BIN) --config $(CONFIG) --check-config

docker-build:
	docker build -t $(IMAGE) .

clean:
	rm -f $(GO_BIN)
