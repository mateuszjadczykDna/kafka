# Include this file first
_empty :=
_space := $(_empty) $(empty)

INIT_CI_TARGETS += ci-install-cpd
RELEASE_TARGETS += $(_empty)
BUILD_TARGETS += $(_empty)
TEST_TARGETS += $(_empty)
CLEAN_TARGETS += $(_empty)

# If this variable is set, release will run make $(RELEASE_MAKE_TARGETS)
RELEASE_MAKE_TARGETS +=

ifeq ($(SEMAPHORE),true)
CI_BIN := $(SEMAPHORE_CACHE_DIR)/bin
else ifeq ($(BUILDKITE),true)
CI_BIN := /tmp/bin
endif

CPD_VERSION := v0.2.0

# Git stuff
BRANCH_NAME ?= $(shell test -d .git && git rev-parse --abbrev-ref HEAD)
# Set RELEASE_BRANCH if we're on master or vN.N.x
RELEASE_BRANCH := $(shell echo $(BRANCH_NAME) | grep -E 'master|v[0-9]+\.[0-9]+\.x')
# assume the remote name is origin by default
GIT_REMOTE_NAME ?= origin

ifeq ($(CI),true)
_ := $(shell test -d $(CI_BIN) || mkdir -p $(CI_BIN))
export PATH = $(CI_BIN):$(shell printenv PATH)
endif

.PHONY: update-mk-include
update-mk-include:
	git subtree pull --prefix mk-include git@github.com:confluentinc/cc-mk-include.git master --squash

.PHONY: bats
bats:
	find . -name *.bats -exec bats {} \;

.PHONY: ci-install-cpd
ci-install-cpd:
	@(test -f $(CI_BIN)/cpd && $(CI_BIN)/cpd version | grep -q $(CPD_VERSION) ) ||\
		aws --profile default s3 cp s3://cloud-confluent-bin/cpd/cpd-$(CPD_VERSION)-linux-amd64 $(CI_BIN)/cpd
	chmod +x $(CI_BIN)/cpd

$(HOME)/.netrc:
	@echo .netrc missing, prompting for user input
	@echo Enter Github credentials, if you use 2 factor authentcation generate a personal access token for the password: https://github.com/settings/tokens
	$(eval user := $(shell bash -c 'read -p "GitHub Username: " user; echo $$user'))
	$(eval pass := $(shell bash -c 'read -p "GitHub Password: " pass; echo $$pass'))
	@printf "machine github.com\n\tlogin $(user)\n\tpassword $(pass)" > $(HOME)/.netrc

.netrc: $(HOME)/.netrc
	cp $(HOME)/.netrc .netrc

.ssh: $(HOME)/.ssh
	cp -R $(HOME)/.ssh .ssh
