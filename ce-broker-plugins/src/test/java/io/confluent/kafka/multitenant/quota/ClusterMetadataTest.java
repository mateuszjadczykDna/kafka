// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.kafka.multitenant.quota;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class ClusterMetadataTest {

  private TestCluster testCluster;

  @Before
  public void setUp() {
    testCluster = new TestCluster();
  }

  @Test
  public void testRackAwareNoRacks() {
    testCluster.addNode(0, null);
    testCluster.addNode(1, null);
    testCluster.addNode(2, null);
    ClusterMetadata clusterMetadata = new ClusterMetadata("tenant1", testCluster.cluster());
    assertFalse(clusterMetadata.rackAware());
  }

  @Test
  public void testRackAwareOneBrokerHasRack() {
    testCluster.addNode(0, null);
    testCluster.addNode(1, "rack1");
    testCluster.addNode(2, null);
    ClusterMetadata clusterMetadata = new ClusterMetadata("tenant1", testCluster.cluster());
    assertFalse(clusterMetadata.rackAware());
  }

  @Test
  public void testRackAwareAllBrokersHasRack() {
    testCluster.addNode(0, "rack1");
    testCluster.addNode(1, "rack2");
    testCluster.addNode(2, "rack3");
    ClusterMetadata clusterMetadata = new ClusterMetadata("tenant1", testCluster.cluster());
    assertTrue(clusterMetadata.rackAware());
  }

  @Test
  public void testNodeReplicaCountsNoTopics() {
    testCluster.addNode(0, null);
    testCluster.addNode(1, null);
    testCluster.addNode(2, null);
    final ClusterMetadata clusterMetadata = new ClusterMetadata("tenant1", testCluster.cluster());
    final Map<Integer, ClusterMetadata.ReplicaCounts> leaderCounts =
        clusterMetadata.nodeReplicaCounts(Collections.emptyList(), true);
    final ClusterMetadata.ReplicaCounts zeroReplicaCounts = new ClusterMetadata.ReplicaCounts(0, 0);
    assertEquals(zeroReplicaCounts, leaderCounts.get(0));
    assertEquals(zeroReplicaCounts, leaderCounts.get(1));
    assertEquals(zeroReplicaCounts, leaderCounts.get(2));

    final Map<Integer, ClusterMetadata.ReplicaCounts> followerCounts =
        clusterMetadata.nodeReplicaCounts(Collections.emptyList(), false);
    assertEquals(zeroReplicaCounts, followerCounts.get(0));
    assertEquals(zeroReplicaCounts, followerCounts.get(1));
    assertEquals(zeroReplicaCounts, followerCounts.get(2));
  }

  @Test
  public void testNodeReplicaCountsOneTenantTopic() {
    final String tenant = "tenant1";
    final String topic = "tenant1_topicA";
    testCluster.addNode(0, null);
    testCluster.addNode(1, null);
    testCluster.addNode(2, null);
    final List<List<Integer>> assignment = ImmutableList.of(
        ImmutableList.of(0, 1, 2), ImmutableList.of(1, 2, 0), ImmutableList.of(2, 0, 1)
    );
    testCluster.createPartitions(topic, 0, assignment);
    ClusterMetadata clusterMetadata = new ClusterMetadata(tenant, testCluster.cluster());

    final ClusterMetadata.ReplicaCounts nodeLeaderCounts = new ClusterMetadata.ReplicaCounts(1, 1);
    Map<Integer, ClusterMetadata.ReplicaCounts> leaderCounts =
        clusterMetadata.nodeReplicaCounts(Collections.emptyList(), true);
    assertEquals(nodeLeaderCounts, leaderCounts.get(0));
    assertEquals(nodeLeaderCounts, leaderCounts.get(1));
    assertEquals(nodeLeaderCounts, leaderCounts.get(2));

    final ClusterMetadata.ReplicaCounts nodeFollowerCounts =
        new ClusterMetadata.ReplicaCounts(2, 2);
    Map<Integer, ClusterMetadata.ReplicaCounts> followerCounts =
        clusterMetadata.nodeReplicaCounts(Collections.emptyList(), false);
    assertEquals(nodeFollowerCounts, followerCounts.get(0));
    assertEquals(nodeFollowerCounts, followerCounts.get(1));
    assertEquals(nodeFollowerCounts, followerCounts.get(2));
  }

  @Test
  public void testNodeReplicaCountsMultipleTenants() {
    final String tenant1 = "tenant1";
    final String topic1 = "tenant1_topicA";
    final String topic2 = "tenant2_topicA";
    testCluster.addNode(0, null);
    testCluster.addNode(1, null);
    testCluster.addNode(2, null);
    final List<List<Integer>> assignment = ImmutableList.of(
        ImmutableList.of(0, 1, 2), ImmutableList.of(1, 2, 0), ImmutableList.of(2, 0, 1)
    );
    testCluster.createPartitions(topic1, 0, assignment);
    testCluster.createPartitions(topic2, 0, assignment);
    ClusterMetadata clusterMetadata = new ClusterMetadata(tenant1, testCluster.cluster());

    final ClusterMetadata.ReplicaCounts nodeLeaderCounts = new ClusterMetadata.ReplicaCounts(1, 2);
    Map<Integer, ClusterMetadata.ReplicaCounts> leaderCounts =
        clusterMetadata.nodeReplicaCounts(Collections.emptyList(), true);
    assertEquals(nodeLeaderCounts, leaderCounts.get(0));
    assertEquals(nodeLeaderCounts, leaderCounts.get(1));
    assertEquals(nodeLeaderCounts, leaderCounts.get(2));

    final ClusterMetadata.ReplicaCounts nodeFollowerCounts =
        new ClusterMetadata.ReplicaCounts(2, 4);
    Map<Integer, ClusterMetadata.ReplicaCounts> followerCounts =
        clusterMetadata.nodeReplicaCounts(Collections.emptyList(), false);
    assertEquals(nodeFollowerCounts, followerCounts.get(0));
    assertEquals(nodeFollowerCounts, followerCounts.get(1));
    assertEquals(nodeFollowerCounts, followerCounts.get(2));
  }

  @Test
  public void testNodeReplicaCountsForOneExistingTenantTopic() {
    final String tenant = "tenant1";
    final String topic = "tenant1_topicA";
    testCluster.addNode(0, null);
    testCluster.addNode(1, null);
    testCluster.addNode(2, null);
    final List<List<Integer>> assignment = ImmutableList.of(
        ImmutableList.of(0, 1, 2), ImmutableList.of(1, 2, 0), ImmutableList.of(2, 0, 1),
        ImmutableList.of(0, 1, 2)
    );
    testCluster.createPartitions(topic, 0, assignment);
    ClusterMetadata clusterMetadata = new ClusterMetadata(tenant, testCluster.cluster());

    Map<Integer, ClusterMetadata.ReplicaCounts> leaderCounts =
        clusterMetadata.nodeReplicaCounts(testCluster.cluster().partitionsForTopic(topic), true);
    assertEquals(new ClusterMetadata.ReplicaCounts(2, 2, 2), leaderCounts.get(0));
    assertEquals(new ClusterMetadata.ReplicaCounts(1, 1, 1), leaderCounts.get(1));
    assertEquals(new ClusterMetadata.ReplicaCounts(1, 1, 1), leaderCounts.get(2));

    Map<Integer, ClusterMetadata.ReplicaCounts> followerCounts =
        clusterMetadata.nodeReplicaCounts(testCluster.cluster().partitionsForTopic(topic), false);
    assertEquals(new ClusterMetadata.ReplicaCounts(2, 2, 2), followerCounts.get(0));
    assertEquals(new ClusterMetadata.ReplicaCounts(3, 3, 3), followerCounts.get(1));
    assertEquals(new ClusterMetadata.ReplicaCounts(3, 3, 3), followerCounts.get(2));
  }

  @Test(expected = IllegalStateException.class)
  public void testNodeReplicaCountsForExistingTenantTopicWithInvalidAssignment() {
    final String tenant = "tenant1";
    final String topic = "tenant1_topicA";
    testCluster.addNode(0, null);
    testCluster.addNode(1, null);
    final List<List<Integer>> assignment = ImmutableList.of(
        ImmutableList.of(0, 1, 2), ImmutableList.of(1, 2, 0), ImmutableList.of(2, 0, 1)
    );
    testCluster.createPartitions(topic, 0, assignment);
    ClusterMetadata clusterMetadata = new ClusterMetadata(tenant, testCluster.cluster());
    try {
      clusterMetadata.nodeReplicaCounts(testCluster.cluster().partitionsForTopic(topic), true);
      fail("Expected nodeReplicaCounts() to throw IllegalStateException");
    } catch (IllegalStateException ise) {
      // success
      clusterMetadata.nodeReplicaCounts(testCluster.cluster().partitionsForTopic(topic), false);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testUpdateNodeMetadataWithInvalidAssignment() {
    testCluster.addNode(0, null);
    testCluster.addNode(1, null);
    final ClusterMetadata clusterMetadata = new ClusterMetadata("tenant1", testCluster.cluster());
    clusterMetadata.updateNodeMetadata(ImmutableList.of(ImmutableList.of(0, 1, 2)));
  }

  @Test
  public void testUpdateNodeMetadata() {
    final String tenant = "tenant1";
    testCluster.addNode(0, null);
    testCluster.addNode(1, null);
    testCluster.addNode(2, null);
    final List<List<Integer>> assignment = ImmutableList.of(
        ImmutableList.of(0, 1, 2), ImmutableList.of(1, 2, 0)
    );
    final ClusterMetadata clusterMetadata = new ClusterMetadata(tenant, testCluster.cluster());
    clusterMetadata.updateNodeMetadata(assignment);
    Map<Integer, ClusterMetadata.ReplicaCounts> leaderCounts =
        clusterMetadata.nodeReplicaCounts(Collections.emptyList(), true);
    assertEquals(new ClusterMetadata.ReplicaCounts(1, 1), leaderCounts.get(0));
    assertEquals(new ClusterMetadata.ReplicaCounts(1, 1), leaderCounts.get(1));
    assertEquals(new ClusterMetadata.ReplicaCounts(0, 0), leaderCounts.get(2));

    Map<Integer, ClusterMetadata.ReplicaCounts> followerCounts =
        clusterMetadata.nodeReplicaCounts(Collections.emptyList(), false);
    assertEquals(new ClusterMetadata.ReplicaCounts(1, 1), followerCounts.get(0));
    assertEquals(new ClusterMetadata.ReplicaCounts(1, 1), followerCounts.get(1));
    assertEquals(new ClusterMetadata.ReplicaCounts(2, 2), followerCounts.get(2));
  }

  @Test
  public void testReplicaCountsComparison() {
    final ClusterMetadata.ReplicaCounts counts0 = new ClusterMetadata.ReplicaCounts(1, 2, 4);
    final ClusterMetadata.ReplicaCounts counts1 = new ClusterMetadata.ReplicaCounts(1, 2, 3);
    final ClusterMetadata.ReplicaCounts counts2 = new ClusterMetadata.ReplicaCounts(1, 4, 3);
    final ClusterMetadata.ReplicaCounts counts3 = new ClusterMetadata.ReplicaCounts(3, 2, 3);
    final ClusterMetadata.ReplicaCounts counts4 = new ClusterMetadata.ReplicaCounts(4, 5, 3);
    final ClusterMetadata.ReplicaCounts counts5 = new ClusterMetadata.ReplicaCounts(4, 5, 3);
    assertTrue(counts0.compareTo(counts1) > 0);
    assertTrue(counts0.compareTo(counts2) > 0);
    assertTrue(counts4.compareTo(counts3) > 0);
    assertTrue(counts3.compareTo(counts2) > 0);
    assertEquals(0, counts5.compareTo(counts4));
    assertTrue(counts5.compareTo(counts0) < 0);
  }

  @Test(expected = NullPointerException.class)
  public void testReplicaCountsCompareToThrowsExceptionForNullParameter() {
    final ClusterMetadata.ReplicaCounts counts = new ClusterMetadata.ReplicaCounts(1, 2, 4);
    counts.compareTo(null);
  }
}
