# Confluent Cloud Makefile Includes
This is a set of Makefile include targets that are used in cloud applications.

## Install
Add this repo to your repo with the command:
```shell
git subtree add --prefix mk-include git@github.com:confluentinc/cc-mk-include.git master --squash
```

Then update your makefile like so:

### Go + Docker + Helm Service
```make
SERVICE_NAME := cc-scraper
MAIN_GO := cmd/scraper/main.go
BASE_IMAGE := golang
BASE_VERSION := 1.9

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-go.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-end.mk
```

### Docker + Helm Only Service
```make
IMAGE_NAME := cc-example
BASE_IMAGE := confluent-docker.jfrog.io/confluentinc/caas-base-alpine
BASE_VERSION := v0.6.1

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-helm.mk
include ./mk-include/cc-end.mk
```

## Updating
Once you have the make targets installed, you can update at any time by running

```shell
make update-mk-include
```
