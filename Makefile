PKG          ?= github.com/figment-networks/firehose-tendermint
BUILD_COMMIT ?= $(shell git rev-parse HEAD)
BUILD_TIME   ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ" | tr -d '\n')
BUILD_PATH   ?= firehose-tendermint
LDFLAGS      ?= -s -w -X main.BuildCommit=$(BUILD_COMMIT) -X main.BuildTime=$(BUILD_TIME)

.PHONY: build
build:
	go build -o build/firehose-tendermint -ldflags "$(LDFLAGS)" ./cmd/firehose-tendermint

.PHONY: install
install:
	go install -ldflags "$(LDFLAGS)" ./cmd/firehose-tendermint

.PHONY: build-all
build-all:
	@mkdir -p dist
	@rm -f dist/*
	LDFLAGS="$(LDFLAGS)" ./scripts/buildall.sh

# MallocNanoZone env var fixes panics in racemode on osx
.PHONY: test
test:
	MallocNanoZone=0 \
		go test -race -cover ./...
