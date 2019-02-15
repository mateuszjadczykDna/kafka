IMAGE_NAME := ce-kafka
BASE_IMAGE := confluent-docker.jfrog.io/confluentinc/cc-base
BASE_VERSION := v2.4.0
MASTER_BRANCH := ce-trunk
KAFKA_VERSION := $(shell awk 'sub(/.*version=/,""){print $1}' ./gradle.properties)
VERSION_POST := -$(KAFKA_VERSION)

# Override docker default targets
BUILD_DOCKER_OVERRIDE := build-docker-override
PUSH_DOCKER_OVERRIDE := push-docker-override

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-end.mk

# Custom docker targets
.PHONY: show-docker-all
show-docker-all:
	@echo
	@echo ========================
	@echo "Docker info for ce-kafka:"
	@make VERSION=$(VERSION) show-docker
	@echo
	@echo ========================
	@echo "Docker info for soak_cluster"
	@make VERSION=$(VERSION) -C cc-services/soak_cluster show-docker
	@echo
	@echo ========================
	@echo "Docker info for trogdor"
	@make VERSION=$(VERSION) -C cc-services/trogdor show-docker

.PHONY: build-docker-override
build-docker-override: .netrc .ssh docker-pull-base $(DOCKER_BUILD_PRE) build-docker-ce-kafka build-docker-cc-services show-docker-all

.PHONY: build-docker-ce-kafka
build-docker-ce-kafka:
	docker build --no-cache --build-arg version=$(IMAGE_VERSION) -t $(BUILD_TAG) .
	rm -rf .netrc .ssh

.PHONY: build-docker-cc-services
build-docker-cc-services:
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/soak_cluster build-docker
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/trogdor build-docker

.PHONY: push-docker-latest
push-docker-override: push-docker-latest push-docker-version push-docker-cc-services

.PHONY: push-docker-cc-services
push-docker-cc-services:
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/soak_cluster push-docker
	make VERSION=$(VERSION) BASE_IMAGE=$(IMAGE_REPO)/$(IMAGE_NAME) BASE_VERSION=$(IMAGE_VERSION) -C cc-services/trogdor push-docker
