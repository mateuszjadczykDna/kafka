SERVICE_NAME := soak-clients
IMAGE_NAME := cc-$(SERVICE_NAME)
CHART_NAME := cc-$(SERVICE_NAME)
CODECOV := true
MAIN_GO := ./main.go
GO_OUTDIR ?= bin
GO_TEST_TARGET = test-go
GO_TEST_ARGS = -race -v -cover -p=1
GO_CODECOV_TEST_ARGS = -race -v -cover -p=1

MASTER_BRANCH := master

SOAK_TEST_NAMESPACE ?= soak-tests

DOCKER_BUILD_PRE = sync-mkinclude
.PHONY: sync-mkinclude
sync-mkinclude:
	rsync -avz --delete-after ../../mk-include/ ./.mk-include.local-context-copy

include ./mk-include/cc-begin.mk
ifeq ($(VERSION),)
include ./mk-include/cc-semver.mk
endif
include ./mk-include/cc-go.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-cpd.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-end.mk

$(GO_OUTDIR)/soak-clients:
	go build -o $@ -ldflags $(GO_LDFLAGS) soak_cluster/main.go

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
