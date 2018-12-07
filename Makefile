IMAGE_NAME := ce-kafka
BASE_IMAGE := confluent-docker.jfrog.io/confluentinc/cc-base
BASE_VERSION := v2.3.0
RELEASE_BRANCH := ce-trunk
DEFAULT_BUMP := minor

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-end.mk
