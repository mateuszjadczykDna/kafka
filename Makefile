IMAGE_NAME := ce-kafka
BASE_IMAGE := confluent-docker.jfrog.io/confluentinc/cc-base
BASE_VERSION := v2.3.0
RELEASE_BRANCH := ce-trunk
DEFAULT_BUMP := minor

#Including above to ensure variables overrides work correctly
include ./mk-include/cc-semver.mk
JAVA_VERSION := $(shell docker run -it $(BASE_IMAGE):$(BASE_VERSION) java -version)
KAFKA_VERSION := $(shell awk 'sub(/.*version=/,""){print $1}' ./gradle.properties)
VERSION := $(shell [ -d .git ] && git describe --tags --always --dirty)
VERSION := $(VERSION)-$(KAFKA_VERSION)-$(USER)

include ./mk-include/cc-begin.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-end.mk
