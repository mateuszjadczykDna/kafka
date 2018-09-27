/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.security.test.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import kafka.api.Request;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.requests.UpdateMetadataRequest.PartitionState;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class EmbeddedKafkaCluster {

  private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafkaCluster.class);
  private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected

  private final MockTime time;
  private EmbeddedZookeeper zookeeper = null;
  private ZkUtils zkUtils = null;
  private Properties brokerConfig;
  private KafkaEmbedded[] brokers;

  public EmbeddedKafkaCluster() {
    time = new MockTime(System.currentTimeMillis(), System.nanoTime());
  }

  public void startZooKeeper() {
    logger.debug("Starting a ZooKeeper instance");
    zookeeper = new EmbeddedZookeeper();
    logger.debug("ZooKeeper instance is running at {}", zkConnectString());

  }

  public void startBrokers(int numBrokers, Properties brokerConfig) throws Exception {
    logger.debug("Initiating embedded Kafka cluster startup with config {}", brokerConfig);
    brokers = new KafkaEmbedded[numBrokers];
    this.brokerConfig = brokerConfig;

    zkUtils = ZkUtils.apply(
        zkConnectString(),
        30000,
        30000,
        JaasUtils.isZkSecurityEnabled());

    brokerConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnectString());
    brokerConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
    putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp(), 0);
    putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
    putIfAbsent(brokerConfig, KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);

    for (int i = 0; i < brokers.length; i++) {
      brokerConfig.put(KafkaConfig$.MODULE$.BrokerIdProp(), i);
      logger.debug("Starting a Kafka instance on port {} ...",
          brokerConfig.get(KafkaConfig$.MODULE$.PortProp()));
      brokers[i] = new KafkaEmbedded(brokerConfig, time);

      logger.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
          brokers[i].brokerList(), brokers[i].zookeeperConnect());
    }
  }

  private void putIfAbsent(Properties props, String propertyKey, Object propertyValue) {
    if (!props.containsKey(propertyKey)) {
      brokerConfig.put(propertyKey, propertyValue);
    }
  }

  public void shutdown() {
    if (brokers != null) {
      for (KafkaEmbedded broker : brokers) {
        broker.stop();
      }
    }
    if (zkUtils != null) {
      zkUtils.close();
    }
    if (zookeeper != null) {
      zookeeper.shutdown();
    }
  }

  /**
   * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format. Example:
   * `127.0.0.1:2181`. <p> You can use this to e.g. tell Kafka brokers how to connect to this
   * instance.
   */
  public String zkConnectString() {
    return "127.0.0.1:" + zookeeper.port();
  }

  /**
   * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`. <p> You can use this to
   * tell Kafka producers how to connect to this cluster.
   */
  public String bootstrapServers() {
    return brokers[0].brokerList();
  }

  public void createTopic(String topic, int partitions, int replication) {
    brokers[0].createTopic(topic, partitions, replication, new Properties());
    List<TopicPartition> topicPartitions = new ArrayList<>();
    for (int partition = 0; partition < partitions; partition++) {
      topicPartitions.add(new TopicPartition(topic, partition));
    }
    waitForTopicPartitions(brokers(), topicPartitions);
  }

  private static void waitForTopicPartitions(List<KafkaServer> servers,
      List<TopicPartition> partitions) {
    partitions.forEach(partition -> waitUntilMetadataIsPropagated(servers, partition));
  }

  private static void waitUntilMetadataIsPropagated(List<KafkaServer> servers, TopicPartition tp) {
    try {
      String topic = tp.topic();
      int partition = tp.partition();
      TestUtils.waitForCondition(() ->
        servers.stream().map(server -> server.apis().metadataCache())
            .allMatch(metadataCache -> {
              Option<PartitionState> partInfo = metadataCache.getPartitionInfo(topic, partition);
              if (partInfo.isEmpty()) {
                return false;
              }
              UpdateMetadataRequest.PartitionState metadataPartitionState = partInfo.get();
              return Request.isValidBrokerId(metadataPartitionState.basePartitionState.leader);
            }) , "Metadata for topic=" + topic + " partition=" + partition + " not propagated");
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }
  }

  private List<KafkaServer> brokers() {
    List<KafkaServer> servers = new ArrayList<>();
    for (KafkaEmbedded broker : brokers) {
      servers.add(broker.kafkaServer());
    }
    return servers;
  }
}
