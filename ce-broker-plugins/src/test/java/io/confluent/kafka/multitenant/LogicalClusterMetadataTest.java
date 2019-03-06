// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static io.confluent.kafka.multitenant.Utils.LC_META_ABC;
import static io.confluent.kafka.multitenant.Utils.LC_META_DED;
import static io.confluent.kafka.multitenant.Utils.LC_META_XYZ;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;

public class LogicalClusterMetadataTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testLoadMetadataFromFile() throws IOException {
    final Path metaFile = tempFolder.newFile("lkc-xyz.json").toPath();
    Files.write(metaFile, jsonString(LC_META_XYZ).getBytes());

    // load metadata and verify
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertEquals(LC_META_XYZ, meta);
    assertTrue(meta.isValid());
  }

  @Test
  public void testLoadMetadataWithNonDefaultOverheadAndRequestRate() throws IOException {
    final Path metaFile = tempFolder.newFile("lkc-abc.json").toPath();
    Files.write(metaFile, Utils.logicalClusterJsonString(LC_META_ABC, true, true).getBytes());

    // load metadata and verify
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertEquals(LC_META_ABC, meta);
    assertTrue(meta.isValid());
  }

  @Test
  public void testLifeCycleMetadataOfLiveCluster() throws IOException {
    final Path metaFile = tempFolder.newFile("lkc-xyz.json").toPath();
    Files.write(metaFile, Utils.logicalClusterJsonString(LC_META_XYZ, true, true).getBytes());

    // load metadata and verify that we have lifecycle metadata
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertEquals(LC_META_XYZ, meta);
    assertEquals(meta.lifecycleMetadata().logicalClusterName(), "xyz");
  }

  @Test
  public void testLifeCycleMetadataOfDeadCluster() throws IOException {

    final Path metaFile = tempFolder.newFile("lkc-abs.json").toPath();
    Files.write(metaFile, Utils.logicalClusterJsonString(LC_META_DED, true, true).getBytes());

    // load metadata and verify that we have lifecycle metadata
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertEquals(LC_META_DED, meta);
    assertTrue(meta.lifecycleMetadata().deletionDate().before(new Date()));
  }

  @Test
  public void testLoadMetadataWithNoByteRatesIsInvalid() throws IOException {
    final String lcId = "lkc-fhg";
    final String invalidMeta = "{" +
                                "\"logical_cluster_id\": \"" + lcId + "\"," +
                                "\"physical_cluster_id\": \"pkc-fhg\"," +
                                "\"logical_cluster_name\": \"name\"," +
                                "\"account_id\": \"account\"," +
                                "\"k8s_cluster_id\": \"k8s-cluster\"," +
                                "\"logical_cluster_type\": \"kafka\"" +
                                "}";
    final Path metaFile = tempFolder.newFile(lcId + ".json").toPath();
    Files.write(metaFile, invalidMeta.getBytes());

    // should be able to load valid json
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertNotNull(meta);
    // but not valid metadata
    assertFalse(meta.isValid());
  }

  @Test
  public void testLoadMetadataWithInvalidClusterType() throws IOException {
    final String lcId = "lkc-fhg";
    final String invalidMeta = "{" +
                                "\"logical_cluster_id\": \"" + lcId + "\"," +
                                "\"physical_cluster_id\": \"pkc-fhg\"," +
                                "\"logical_cluster_name\": \"name\"," +
                                "\"account_id\": \"account\"," +
                                "\"k8s_cluster_id\": \"k8s-cluster\"," +
                                "\"logical_cluster_type\": \"not-kafka\"," +
                                "\"storage_bytes\": 100," +
                                "\"network_ingress_byte_rate\": 1024," +
                                "\"network_egress_byte_rate\": 1024" +
                                "}";
    final Path metaFile = tempFolder.newFile(lcId + ".json").toPath();
    Files.write(metaFile, invalidMeta.getBytes());

    // should be able to load valid json
    LogicalClusterMetadata meta = loadFromFile(metaFile);
    assertNotNull(meta);
    // but not valid metadata
    assertFalse(meta.isValid());
    assertEquals(lcId, meta.logicalClusterId());
    assertEquals((Long) 1024L, meta.producerByteRate());
    assertEquals((Long) 1024L, meta.consumerByteRate());
  }

  private LogicalClusterMetadata loadFromFile(Path metaFile) {
    LogicalClusterMetadata retMeta = null;
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      retMeta = objectMapper.readValue(metaFile.toFile(), LogicalClusterMetadata.class);
    } catch (IOException ioe) {
      fail("Failed to read logical cluster metadata from file " + metaFile);
    }
    return retMeta;
  }

  private static String jsonString(LogicalClusterMetadata lcMeta) {
    return Utils.logicalClusterJsonString(lcMeta, false, false);
  }
}
