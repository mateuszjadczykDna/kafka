#
# Copyright 2017 Confluent Inc.
#

# Builds a Confluent Cloud Docker image for Trogdor

ARG base_version
ARG image_version

FROM confluent-docker.jfrog.io/confluentinc/ce-kafka:latest

EXPOSE 8881

ENV COMPONENT="trogdor"
ENV RUN_CLASS="org.apache.kafka.trogdor.agent.Agent"
ENV CLASS_ARGS="-h"
ENV KAFKA_SECRETS_DIR=/mnt/secrets
ENV KAFKA_CONFIG_DIR=/mnt/config

COPY include/opt/caas /opt/caas

CMD ["/opt/caas/bin/run"]
