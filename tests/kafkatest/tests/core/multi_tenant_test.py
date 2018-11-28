# Copyright 2017 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark import ignore

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int

INTERCEPTOR_CLASS_PROP = "broker.interceptor.class"
PRINCIPAL_BUILDER_CLASS_PROP = "principal.builder.class"
OAUTHBEARER_SASL_SERVER_CALLBACK_CLASS_PROP = "oauthbearer.sasl.server.callback.handler.class"


def listener_prop(listener, prop):
    return "listener.name.%s.%s" % (listener, prop)


class MultiTenantTest(ProduceConsumeValidateTest):
    OAUTH_SECURED_JWS_TOKEN_PUBLIC_KEY_PATH = "/mnt/test/jws_public_key.pem"
    JWS_TOKEN = (
        "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJDb25mbHVlbnQiLCJhdWQiOm51bGwsImV4cCI"
        "6MTkzNTA1Nzc4OCwianRpIjoid0EtVWp1MzJEcFJtNXlmMWVWeFIwUSIsImlhdCI6MTU"
        "zNTcwODk3OSwibmJmIjoxNTM1NzA4ODU5LCJzdWIiOiJDb25mbHVlbnQgRW50ZXJwcml"
        "zZSIsIm1vbml0b3JpbmciOnRydWUsImNsdXN0ZXJzIjpbImNwMTIiLCJjcDEzIl19.jM"
        "FXKrWsUdGM_ckLC5lEHudSYZIfrM9urRXtZ62FoGDL2VeG_BRBkRX4paUh29EXluMwEj"
        "pLxth0gqyqlvfkhgeh9jALcvaTGKzOPeZYGWQCJwoNobwDxw49EuI9uzdXqpIaPhDqv8"
        "DEny52_xI3Lt3HodK-zvUGR9FVNxejy4HYw9i7vCLjHgzjMtGoOn2rDFsJrEiJzMdm4z"
        "3TQVG6H2zMQ3-3uER18HSGtqOs1j2M2SdL2KxRwoR1SajUwNHPzvAATWGWbeuo-sJVpq"
        "MiE0spBsZg-eG28P4PTgMKkT9sZkAnaTGS5lRx7NfFuE3WeLb4EuaAg3F5iGoInHxcaA"
    )
    ALLOWED_CLUSTER = 'cp12'
    OAUTHBEARER_LOGIN_CALLBACK_HANDLER_CLASS = 'io.confluent.kafka.clients.plugins.auth.oauth.' \
                                               'OAuthBearerLoginCallbackHandler'
    OAUTHBEARER_SERVER_CALLBACK_HANDLER_CLASS = 'io.confluent.kafka.server.plugins.auth.oauth.' \
                                                'OAuthBearerValidatorCallbackHandler'

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(MultiTenantTest, self).__init__(test_context=test_context)
        self.client_listener = "SASL_PLAINTEXT"
        self.broker_listener = "PLAINTEXT"

        # For now, we depend on auto-creation of this topic
        self.topic = "test_topic"
        self.consumer_group = "test_group"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.tenant_prefix = self.ALLOWED_CLUSTER

        server_jaas_config = """org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \
            unsecuredLoginStringClaim_sub="Confluent" \
            publicKeyPath="{key_path}";""".format(key_path=self.OAUTH_SECURED_JWS_TOKEN_PUBLIC_KEY_PATH)

        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                  security_protocol=self.client_listener,
                                  client_sasl_mechanism="OAUTHBEARER",
                                  interbroker_security_protocol=self.broker_listener,
                                  server_prop_overides=[
                                      [listener_prop("sasl_plaintext", INTERCEPTOR_CLASS_PROP),
                                       "io.confluent.kafka.multitenant.MultiTenantInterceptor"],
                                      [listener_prop("sasl_plaintext", PRINCIPAL_BUILDER_CLASS_PROP),
                                       "io.confluent.kafka.multitenant.MultiTenantPrincipalBuilder"],
                                      [listener_prop("sasl_plaintext", OAUTHBEARER_SASL_SERVER_CALLBACK_CLASS_PROP),
                                       self.OAUTHBEARER_SERVER_CALLBACK_HANDLER_CLASS],
                                      [listener_prop("sasl_plaintext", "oauthbearer.sasl.jaas.config"),
                                       server_jaas_config]
                                  ])
        client_jaas_config_variables = {
            'allowed_cluster': self.ALLOWED_CLUSTER,
            'jws_token': self.JWS_TOKEN
        }

        self.producer = VerifiableProducer(test_context, 1, self.kafka, self.topic,
                                           jaas_override_variables=client_jaas_config_variables)
        self.producer.sasl_login_callback_handler_class = self.OAUTHBEARER_LOGIN_CALLBACK_HANDLER_CLASS

        self.consumer = ConsoleConsumer(self.test_context, 1,
                                        self.kafka, self.topic,
                                        group_id=self.consumer_group,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int,
                                        jaas_override_variables=client_jaas_config_variables)
        self.consumer.sasl_login_callback_handler_class = self.OAUTHBEARER_LOGIN_CALLBACK_HANDLER_CLASS

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def list_consumer_groups(self, listener):
        node = self.kafka.nodes[0]
        consumer_group_script = self.kafka.path.script("kafka-consumer-groups.sh", node)
        cmd = "%s --bootstrap-server %s --list" % \
              (consumer_group_script, self.kafka.bootstrap_servers(listener))
        for line in node.account.ssh_capture(cmd):
            if line.startswith("SLF4J") or line.startswith("Note"):
                continue

            cleaned_line = line.rstrip()
            if cleaned_line:
                yield cleaned_line

    def test_basic_produce_and_consume(self):
        self.run_produce_consume_validate()

        # Assert that the topic was created with the tenant prefix
        topics = set(self.kafka.list_topics(None)).difference({"__consumer_offsets"})

        assert {"{0}_{1}".format(self.tenant_prefix, self.topic)} == set(topics), \
            "Found unexpected topics: %s" % str(topics)

        # Assert that the consumer group was created with the tenant prefix
        consumer_groups = set(self.list_consumer_groups(self.broker_listener))
        assert {"{0}_{1}".format(self.tenant_prefix, self.consumer_group)} == consumer_groups, \
            "Found unexpected consumer groups: %s" % str(consumer_groups)
