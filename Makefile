PKG          ?= github.com/figment-networks/firehose-cosmos
BUILD_COMMIT ?= $(shell git rev-parse HEAD)
BUILD_TIME   ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ" | tr -d '\n')
BUILD_PATH   ?= firehose-cosmos
LDFLAGS      ?= -s -w -X main.BuildCommit=$(BUILD_COMMIT) -X main.BuildTime=$(BUILD_TIME)
VERSION      ?= 0.1.0
DOCKER_IMAGE ?= figmentnetworks/firehose-cosmos
DOCKER_TAG   ?= ${VERSION}

.PHONY: build
build:
	go build -o build/firehose-cosmos -ldflags "$(LDFLAGS)" ./cmd/firehose-cosmos

.PHONY: install
install:
	go install -ldflags "$(LDFLAGS)" ./cmd/firehose-cosmos

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

.PHONY: docker-build
docker-build:
	docker build \
		--platform linux/amd64 \
		--build-arg VERSION=$(VERSION) \
		-t ${DOCKER_IMAGE}:${DOCKER_TAG} \
		.

.PHONY: docker-push
docker-push:
	docker push ${DOCKER_IMAGE}:${DOCKER_TAG}
