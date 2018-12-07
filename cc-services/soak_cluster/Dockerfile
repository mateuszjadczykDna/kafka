FROM confluent-docker.jfrog.io/confluentinc/cc-service-base:1.9

ARG version

WORKDIR /root
COPY .ssh .ssh
COPY .netrc ./
RUN ssh-keyscan -t rsa github.com > /root/.ssh/known_hosts

WORKDIR /go/src/github.com/confluentinc/ce-kafka/cc-services/soak_cluster
COPY mk-include ./mk-include
COPY Gopkg.toml Gopkg.lock Makefile ./
COPY . .
RUN make deps DEP_ARGS=-vendor-only

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build-go GO_OUTDIR= VERSION=${version}

FROM confluent-docker.jfrog.io/confluentinc/cc-built-base:0.0.2

COPY --from=0 /soak-clients /

ENTRYPOINT ["/soak-clients"]
