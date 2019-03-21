/*
 Copyright 2018 Confluent Inc.
 */

package integration.kafka.tier

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.{Properties, UUID}

import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{ConfluentTopicConfig, TopicConfig}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{Before, Ignore, Test}

import scala.collection.JavaConverters._

class TierIntegrationFetchTest extends IntegrationTestHarness {
  override protected def serverCount: Int = 1

  private def configureMock = {
    serverConfig.put(KafkaConfig.TierBackendProp, "mock")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
  }

  private def configureMinio = {
    serverConfig.put(KafkaConfig.TierBackendProp, "S3")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "mybucket")
    serverConfig.put(KafkaConfig.TierS3AwsAccessKeyIdProp, "admin")
    serverConfig.put(KafkaConfig.TierS3AwsSecretAccessKeyProp, "password")
    serverConfig.put(KafkaConfig.TierS3RegionProp, "us-east-1")
    serverConfig.put(KafkaConfig.TierS3EndpointOverrideProp, "http://localhost:9000")
    serverConfig.put(KafkaConfig.TierS3SignerOverrideProp, "AWSS3V4SignerType")
    serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  }

  private def configures3GcsCompatMode = {
    serverConfig.put(KafkaConfig.TierBackendProp, "S3")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "tiered-storage-gcs-compatibility-testing-lucas")
    serverConfig.put(KafkaConfig.TierS3RegionProp, "us-east-1")
    serverConfig.put(KafkaConfig.TierS3EnableMultipartUploadProp, "false")
    serverConfig.put(KafkaConfig.TierS3EndpointOverrideProp, "storage.googleapis.com")
    serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  }

  private def configureS3 = {
    serverConfig.put(KafkaConfig.TierBackendProp, "S3")
    serverConfig.put(KafkaConfig.TierS3BucketProp, "ai383estnar")
    serverConfig.put(KafkaConfig.TierS3RegionProp, "us-west-2")
    serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  }

  serverConfig.put(KafkaConfig.TierEnableProp, "false")
  serverConfig.put(KafkaConfig.TierFeatureProp, "true")
  serverConfig.put(KafkaConfig.TierMetadataNumPartitionsProp, "1")
  serverConfig.put(KafkaConfig.TierMetadataReplicationFactorProp, "1")
  serverConfig.put(KafkaConfig.LogCleanupIntervalMsProp, "500")
  serverConfig.put(KafkaConfig.TierLocalHotsetBytesProp, "0")
  //configureMinio
  //configureS3
  //configures3GcsCompatMode
  configureMock

  private val topic = UUID.randomUUID().toString

  @Before
  override def setUp(): Unit = {
    super.setUp()
    val props = new Properties
    props.put(ConfluentTopicConfig.TIER_ENABLE_CONFIG, "true")
    props.put(TopicConfig.SEGMENT_BYTES_CONFIG, "5000000")
    // Set hotset retention bytes adequately low, to allow us to delete some segments after they have been tiered
    props.put(ConfluentTopicConfig.TIER_LOCAL_HOTSET_BYTES_CONFIG, "10000")
    props.put(TopicConfig.RETENTION_BYTES_CONFIG, "-1")

    createTopic(topic, 2, 1, props)
  }

  @Test
  @Ignore
  def testArchiveAndFetch(): Unit = {
    val producer = createProducer()

    val nBatches = 10000
    val recordsPerBatch = 100

    for (b <- 0 until nBatches) {
      val producerRecords = (0 until recordsPerBatch).map(i => {
        val m = recordsPerBatch * b + i
        new ProducerRecord(topic, null, null,
          "abcdefghijklmnopqrstuvwxyz0123456789".getBytes(StandardCharsets.UTF_8),
           s"$m".getBytes(StandardCharsets.UTF_8)
        )
      })
      producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))
    }

    val topicPartition = new TopicPartition(topic, 0)

    TestUtils.waitUntilTrue(() => {
      servers.head.tierMetadataManager.tierPartitionState(topicPartition).get().numSegments() > 2
    }, "timeout waiting for at least two segments to be archived and materialized", 60000L)

    val replicaManager = servers.head.replicaManager
    val log = replicaManager.logManager.getLog(topicPartition)

    val deletedSegments = log.get.deleteOldSegments()
    assertTrue("tiered segments should have been deleted", deletedSegments > 0)

    val brokerList = TestUtils.bootstrapServers(servers, listenerName)

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50000")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    try {
      val partitions = new util.ArrayList[TopicPartition]()
      partitions.add(topicPartition)
      partitions.add(new TopicPartition(topic, 1))
      consumer.assign(partitions)
      consumer.seekToBeginning(partitions)
      val valuesRead = new util.ArrayList[Int]()

      while (valuesRead.size() != nBatches * recordsPerBatch) {
        val records = consumer.poll(Duration.ofMillis(1000))
        records.forEach(new Consumer[ConsumerRecord[String, String]]() {
          override def accept(r: ConsumerRecord[String, String]): Unit = {
            valuesRead.add(Integer.parseInt(r.value()))
          }
        })
      }
      val expectedValues = new util.ArrayList[Int](Range(0, nBatches * recordsPerBatch).asJava)
      assertEquals(expectedValues, valuesRead)

    } finally {
      consumer.close()
    }
  }
}
