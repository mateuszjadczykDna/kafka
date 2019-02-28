SERVICE_NAME := trogdor
IMAGE_NAME := cc-$(SERVICE_NAME)
CHART_NAME := cc-$(SERVICE_NAME)-service
RELEASE_BRANCH := master
DEFAULT_BUMP := minor

BASE_IMAGE ?= confluent-docker.jfrog.io/confluentinc/ce-kafka
# Use master clean version as docker base verson unless specified
BASE_VERSION ?= $(shell git rev-parse --is-inside-work-tree > /dev/null && git describe --tags --always master)

SOAK_TEST_NAMESPACE ?= soak-tests

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-cpd.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-end.mk

.PHONY: helm-deploy-soak
## Deploy helm to current kube context with values set to local.yaml on the soak-tests namespace
helm-deploy-soak:
	helm upgrade \
	--install $(CHART_NAME)-dev \
	charts/$(CHART_NAME) \
	--namespace $(SOAK_TEST_NAMESPACE) \
	--set namespace=$(SOAK_TEST_NAMESPACE) \
	--debug \
	-f charts/values/local.yaml \
	--set image.tag=$(CHART_VERSION) $(HELM_ARGS)
