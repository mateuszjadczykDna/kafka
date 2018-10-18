IMAGE_NAME := confluentinc/cc-kafka
IMAGE_VERSION=$(shell git describe --tags --always --long --dirty)
DOCKER_REPO ?= 368821881613.dkr.ecr.us-west-2.amazonaws.com
BASE_IMAGE := ${DOCKER_REPO}/confluentinc/cc-base
BASE_VERSION := v2.1.0
AWS_PROFILE ?= default
AWS_REGION ?= us-west-2
GIT_SHA=$(shell git rev-parse HEAD)
GIT_BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
CONFLUENT_VERSION ?= 5.1.0-SNAPSHOT
LIFECYCLE_POLICY ?= '{"rules":[{"rulePriority":10,"description":"keeps 50 latest tagged images","selection":{"tagStatus":"tagged","countType":"imageCountMoreThan","countNumber":50,"tagPrefixList":["v"]},"action":{"type":"expire"}},{"rulePriority":20,"description":"keeps 5 latest untagged images","selection":{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":5},"action":{"type":"expire"}},{"rulePriority":30,"description":"keeps latest 20 numeric-tagged images","selection":{"tagStatus":"tagged","countType":"imageCountMoreThan","tagPrefixList":["0","1","2","3","4","5","6","7","8","9"],"countNumber":20},"action":{"type":"expire"}},{"rulePriority":40,"description":"keeps latest 20 a-f tagged images","selection":{"tagStatus":"tagged","countType":"imageCountMoreThan","tagPrefixList":["a","b","c","d","e","f"],"countNumber":20},"action":{"type":"expire"}}]}'

show-args:
	@echo 'IMAGE_NAME: $(IMAGE_NAME)'
	@echo 'IMAGE_VERSION: $(IMAGE_VERSION)'
	@echo 'DOCKER_REPO: $(DOCKER_REPO)'
	@echo 'BASE_IMAGE: $(BASE_IMAGE)'
	@echo 'BASE_VERSION: $(BASE_VERSION)'

FORCE:

build-docker: pull-base
	docker build --no-cache --build-arg version=$(IMAGE_VERSION) --build-arg kafka_version=$(KAFKA_VERSION) --build-arg CC_BASE_VERSION=$(BASE_VERSION) --build-arg confluent_version=$(CONFLUENT_VERSION) --build-arg git_sha=$(GIT_SHA) --build-arg git_branch=$(GIT_BRANCH) -t $(IMAGE_NAME):$(IMAGE_VERSION) .

repo-login:
	@eval "$(shell aws ecr get-login --no-include-email --region us-west-2 --profile default)"

create-repo:
	aws ecr describe-repositories --region us-west-2 --repository-name $(IMAGE_NAME) || aws ecr create-repository --region us-west-2 --repository-name $(IMAGE_NAME)
	aws ecr put-lifecycle-policy --region us-west-2 --repository-name $(IMAGE_NAME) --lifecycle-policy-text $(LIFECYCLE_POLICY) || echo "Failed to put lifecycle policy on $(IMAGE_NAME) repo"

pull-base: repo-login
ifneq ($(BASE_IMAGE),$(_empty))
	docker image ls -f reference="$(BASE_IMAGE):$(BASE_VERSION)" | grep -Eq "$(BASE_IMAGE)[ ]*$(BASE_VERSION)" || \
		docker pull $(BASE_IMAGE):$(BASE_VERSION)
endif

push-docker: create-repo push-docker-latest push-docker-version

push-docker-latest: tag-docker-latest
	@echo 'push latest to $(DOCKER_REPO)'
	docker push $(DOCKER_REPO)/$(IMAGE_NAME):latest

push-docker-version: tag-docker-version
	@echo 'push $(IMAGE_VERSION) to $(DOCKER_REPO)'
	docker push $(DOCKER_REPO)/$(IMAGE_NAME):$(IMAGE_VERSION)

tag-docker-latest:
	@echo 'create docker tag latest'
	docker tag $(IMAGE_NAME):$(IMAGE_VERSION) $(DOCKER_REPO)/$(IMAGE_NAME):latest

tag-docker-version:
	@echo 'create docker tag $(IMAGE_VERSION)'
	docker tag $(IMAGE_NAME):$(IMAGE_VERSION) $(DOCKER_REPO)/$(IMAGE_NAME):$(IMAGE_VERSION)
