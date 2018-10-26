// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.quota;

import io.confluent.kafka.multitenant.schema.TenantContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates cluster metadata used for partition assignment including tenant-specific
 * partition counts.
 */
class ClusterMetadata {

  private static final Logger logger = LoggerFactory.getLogger(ClusterMetadata.class);

  final Set<Integer> brokers;
  final Set<String> racks;
  final Map<Integer, String> brokerRackMap;
  private final String tenant;
  private final Cluster cluster;
  private final Map<Node, NodeMetadata> nodeMetadatas;

  ClusterMetadata(String tenant, Cluster cluster) {
    this.cluster = cluster;
    this.tenant = tenant;
    this.brokers = cluster.nodes().stream().map(Node::id).collect(Collectors.toSet());
    nodeMetadatas = nodeMetadata();

    if (rackAware()) {
      racks = new HashSet<>();
      brokerRackMap = new HashMap<>();
      final Map<String, List<Integer>> brokersByRack = new HashMap<>(racks.size());
      for (Node node : cluster.nodes()) {
        String rack = node.rack();
        brokerRackMap.put(node.id(), rack);
        racks.add(rack);
        brokersByRack.putIfAbsent(rack, new ArrayList<>());
        brokersByRack.get(rack).add(node.id());
      }
    } else {
      this.racks = Collections.emptySet();
      this.brokerRackMap = Collections.emptyMap();
    }
  }

  /**
   * Returns true if the all brokers in the cluster have rack specified.
   * If true, rack-aware assignment will be used.
   */
  boolean rackAware() {
    return cluster.nodes().stream().noneMatch(node -> node.rack() == null);
  }

  /**
   * Returns the replica counts for all nodes including any existing partitions of the topic
   * if this is a CreatePartitionsRequest.
   *
   * @param existingPartitions Existing partition info for CreatePartitionsRequest
   * @param leader true if leader is being assigned, false otherwise
   * @return nodeId => replica count mapping
   */
  Map<Integer, ReplicaCounts> nodeReplicaCounts(List<PartitionInfo> existingPartitions,
      boolean leader) {

    Map<Integer, ReplicaCounts> nodeReplicaCounts = new HashMap<>(nodeMetadatas.size());
    for (Map.Entry<Node, NodeMetadata> entry : nodeMetadatas.entrySet()) {
      Node node = entry.getKey();
      NodeMetadata nodeMetadata = entry.getValue();
      int tenantCount = leader ? nodeMetadata.tenantLeaders
          : nodeMetadata.tenantFollowers;
      int totalCount = leader ? nodeMetadata.totalLeaders
          : nodeMetadata.totalFollowers;
      ReplicaCounts replicaCounts = new ReplicaCounts(tenantCount, totalCount);
      nodeReplicaCounts.put(node.id(), replicaCounts);
    }
    for (PartitionInfo partitionInfo : existingPartitions) {
      Node[] replicas = partitionInfo.replicas();
      for (int i = 0; i < replicas.length; i++) {
        ReplicaCounts replicaCounts = nodeReplicaCounts.get(replicas[i].id());
        if (replicaCounts != null) {
          if ((i == 0 && leader) || (i != 0 && !leader)) {
            replicaCounts.topic++;
          }
        } else {
          throw new IllegalStateException("Inconsistent cluster metadata, broker not found");
        }
      }
    }
    return nodeReplicaCounts;
  }

  /**
   * Update node metadata with tenant and total replica counts corresponding to
   * the provided assignment
   * @param assignment assignment being added
   */
  void updateNodeMetadata(List<List<Integer>> assignment) {
    for (List<Integer> replicas : assignment) {
      for (int i = 0; i < replicas.size(); i++) {
        Node replicaNode = cluster.nodeById(replicas.get(i));
        NodeMetadata nodeMetadata = nodeMetadatas.get(replicaNode);
        if (nodeMetadata != null) {
          if (i == 0) {
            nodeMetadata.tenantLeaders++;
            nodeMetadata.totalLeaders++;
          } else {
            nodeMetadata.tenantFollowers++;
            nodeMetadata.totalFollowers++;
          }
        } else {
          logger.error("Inconsistent cluster metadata: replica node {} not found", replicaNode);
        }
      }
    }
  }

  /**
   * Returns the node metadata for the cluster including leader and follower
   * replica counts used to order nodes for partition assignment.
   */
  private Map<Node, NodeMetadata> nodeMetadata() {
    List<Node> nodes = cluster.nodes();
    Map<Node, NodeMetadata> nodeMetadatas = new HashMap<>(nodes.size());
    for (Node node : nodes) {
      nodeMetadatas.put(node, new NodeMetadata());
    }
    String tenantPrefix = tenant + TenantContext.DELIMITER;
    for (String topic : cluster.topics()) {
      boolean isTenantTopic = topic.startsWith(tenantPrefix);
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        Node[] replicas = partitionInfo.replicas();
        for (int i = 0; i < replicas.length; i++) {
          Node replicaNode = replicas[i];
          if (replicaNode != null) {
            NodeMetadata nodeMetadata = nodeMetadatas.get(replicaNode);
            if (nodeMetadata != null) {
              if (i == 0) {
                nodeMetadata.totalLeaders++;
                if (isTenantTopic) {
                  nodeMetadata.tenantLeaders++;
                }
              } else {
                nodeMetadata.totalFollowers++;
                if (isTenantTopic) {
                  nodeMetadata.tenantFollowers++;
                }
              }
            } else {
              logger.error("Inconsistent cluster metadata: replica node {} not found", replicaNode);
            }
          }
        }
      }
    }
    return nodeMetadatas;
  }

  /**
   * Node metadata including leader and follower counts (total and for the tenant)
   */
  private static class NodeMetadata {
    int tenantLeaders;
    int tenantFollowers;
    int totalLeaders;
    int totalFollowers;
  }

  /**
   * Replica counts at different levels used for ordering nodes for partition assignment.
   * Multiple levels are used to make the decision when there is a choice.
   *
   * <p>Goals:
   * 1) We want to balance partitions of a topic. If we have 8 nodes and are assigning 16 partitions
   *    of a topic, then each node gets two partitions. This is done using the topic count.
   * 2) If we are assigning a topic with 2 partitions, we have a choice of 8 nodes.
   *    For example, a tenant has 14 partitions and we are allocating a topic with 2 partitions.
   *    Rather than randomly choose 2 nodes, we choose the 2 nodes with 1 tenant partition each.
   *    This is done using the tenant count.
   * 3) If we have 16 tenant partitions balanced across 8 nodes and we are assigning a topic with
   *    2 partitions, then we have a choice of 8 nodes. Tenant partitions are already balanced.
   *    We choose the 2 nodes with least total number of partitions. This is done using the
   *    total count.
   *
   * <p>ReplicaCounts may be either for:
   * 1) leaders: Used for leader allocation, takes only leader replicas into account
   * 2) followers: Used for follower allocation, uses total of all follower replicas
   *
   * <p>The replicas are counted at three levels, in increasing order of priority:
   * 1) topic: Existing replicas of this topic on this broker if request is CreatePartitions
   * 2) tenant: Number of replicas of this tenant allocated on this broker
   * 3) total: Total number of replicas on this broker
   */
  static class ReplicaCounts implements Comparable<ReplicaCounts> {
    int topic;
    int tenant;
    int total;

    ReplicaCounts(int tenant, int total) {
      this.tenant = tenant;
      this.total = total;
    }

    void incrementCounts() {
      topic++;
      tenant++;
      total++;
    }

    @Override
    public int compareTo(ReplicaCounts o) {
      int result = Integer.compare(topic, o.topic);
      if (result == 0) {
        result = Integer.compare(tenant, o.tenant);
      }
      if (result == 0) {
        result = Integer.compare(total, o.total);
      }
      return result;
    }
  }
}
