// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.quota;

import io.confluent.kafka.multitenant.quota.ClusterMetadata.ReplicaCounts;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tenant partition assignor that attempts to balance tenant partitions across available
 * brokers to ensure that quotas allocated to the broker per-partition can be fully
 * utilized without overloading brokers.
 *
 * <p>CreateTopicsRequest and CreatePartitionsRequest are processed only on the controller.
 * So only one broker computes assignments at any one time. To avoid reading all topics
 * from ZooKeeper for every request, we use the cluster obtained from MetadataCache.
 * If cluster/topic information is not available, we fallback to default assignment in
 * the broker.</p>
 *
 * <p>Note: Delays in propagating metadata can result in slightly imbalanced assignments.
 *
 * <p>This assignor attempts to distribute partitions of each topic across available
 * brokers, taking racks into account for replica placement. Assignment algorithm for
 * partitions that are a multiple of the number of brokers is similar to the default
 * replica assignment used by brokers. For the remainder of the partitions, current
 * tenant assignments are taken into account to achieve balanced allocation for tenants.</p>
 */
public class TenantPartitionAssignor {
  private static final Logger logger = LoggerFactory.getLogger(TenantPartitionAssignor.class);

  private volatile Cluster cluster;

  public void updateClusterMetadata(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * Assign partitions for a CreateTopics request.
   * @param tenant Tenant corresponding to the request
   * @param newTopics Topic metadata for new topics including partition count and replication factor
   * @return Partition assignments for the new topics
   */
  public Map<String, List<List<Integer>>> assignPartitionsForNewTopics(String tenant,
      Map<String, TopicInfo> newTopics) {
    Cluster cluster = this.cluster;
    if (cluster == null || cluster.nodes().isEmpty()) {
      return defaultAssignment(newTopics.keySet(), Collections.emptyList());
    }
    return assignPartitions(cluster, tenant, newTopics);
  }

  /**
   * Assign partitions for a CreatePartitions request.
   * @param tenant Tenant corresponding to the request
   * @param partitionCounts Total partition count for each topic after partitions are created.
   * @return Partition assignments for the new partitions of each topic
   */
  public Map<String, List<List<Integer>>> assignPartitionsForExistingTopics(String tenant,
      Map<String, Integer> partitionCounts) {
    Cluster cluster = this.cluster;
    if (cluster == null || cluster.nodes().isEmpty()) {
      return defaultAssignment(partitionCounts.keySet(), null);
    }
    Map<String, List<List<Integer>>> result = new HashMap<>(partitionCounts.size());
    Map<String, TopicInfo> topicInfos = new HashMap<>();
    for (Map.Entry<String, Integer> entry : partitionCounts.entrySet()) {
      String topic = entry.getKey();
      int totalPartitions = entry.getValue();
      List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
      if (!partitionInfos.isEmpty()) {
        int startPartition = partitionInfos.size();
        short replication = (short) partitionInfos.get(0).replicas().length;
        if (startPartition < totalPartitions) {
          topicInfos.put(topic, new TopicInfo(totalPartitions, replication, startPartition));
        } else {
          logger.debug("Topic metadata out-of-date for {}, using default assignment, "
              + " startPartition is {} and requested totalPartitions is {}",
              topic, startPartition, totalPartitions);
          result.put(topic, Collections.emptyList());
        }
      } else {
        logger.debug("Topic metadata not available for {}, using default assignment", topic);
        result.put(topic, Collections.emptyList());
      }
    }
    if (!topicInfos.isEmpty()) {
      result.putAll(assignPartitions(cluster, tenant, topicInfos));
    }
    return result;
  }

  /**
   * Performs tenant-aware partition assignment for a CreateTopics or CreatePartitions request.
   *
   * @param cluster Cluster metadata including all active nodes and existing partition assignments
   * @param tenant Tenant whose partitions are being assigned
   * @param topics topic/partition metadata
   * @return Balanced partition assignment
   */
  private Map<String, List<List<Integer>>> assignPartitions(Cluster cluster,
                                                            String tenant,
                                                            Map<String, TopicInfo> topics) {
    Map<String, List<List<Integer>>> result = new HashMap<>(topics.size());
    ClusterMetadata clusterMetadata = new ClusterMetadata(tenant, cluster);
    for (Map.Entry<String, TopicInfo> entry : topics.entrySet()) {
      String topic = entry.getKey();
      TopicInfo topicInfo = entry.getValue();
      if (topicInfo.replicationFactor <= cluster.nodes().size()) {
        List<PartitionInfo> partitionInfos = topicInfo.isNewTopic()
            ?  Collections.emptyList()
            : cluster.partitionsForTopic(topic);
        Map<Integer, ReplicaCounts> leaderReplicaCounts =
            clusterMetadata.nodeReplicaCounts(partitionInfos, true);
        Map<Integer, ReplicaCounts> followerReplicaCounts =
            clusterMetadata.nodeReplicaCounts(partitionInfos, false);
        List<List<Integer>> assignment;
        if (clusterMetadata.rackAware()) {
          assignment = assignReplicasToBrokersRackAware(clusterMetadata, topicInfo,
              leaderReplicaCounts, followerReplicaCounts);
        } else {
          assignment = assignReplicasToBrokersRackUnaware(clusterMetadata, topicInfo,
              leaderReplicaCounts, followerReplicaCounts);
        }
        clusterMetadata.updateNodeMetadata(assignment);
        result.put(topic, assignment);
      } else {
        logger.info("Insufficient nodes {} for assignment of topic {}, with replication factor "
            + "{} , using default assignment", cluster.nodes().size(),
            topic, topicInfo.replicationFactor);
        result.put(topic, Collections.emptyList());
      }
    }
    return result;
  }

  /**
   * Returns empty assignment that results in the default broker partition assignor to be used.
   * This is only used if the cluster metadata required to perform tenant-aware assignment is
   * not available at the time the request is processed.
   */
  private Map<String, List<List<Integer>>> defaultAssignment(Set<String> topics,
      List<List<Integer>> defaultValue) {
    logger.info("Cluster info not available, using default partition assignment for {}", topics);
    return topics.stream()
        .collect(Collectors.toMap(Function.identity(), t -> defaultValue));
  }

  /**
   * Rack-unaware tenant-aware replica assignment.
   *
   * <p>The goals of this assignment:
   * 1. Spread partition replicas of a topic evenly among brokers.
   * 2. Spread replicas of a tenant evenly among brokers.
   * 3. Spread replicas evenly among brokers if possible (1. and 2. are attempted first).
   * 4. For partitions assigned to a particular broker, their other replicas are spread over
   *    the other brokers.
   *
   * <p>To achieve these goals for replica assignment without considering racks, we:
   * 1. Assign the first replica of each partition by round-robin, starting from the node with the
   *    least number of leaders
   * 2. Assign the remaining replicas of each partition with an increasing shift numBrokers at
   *    a time
   * 3. Assign remaining follower replicas based on follower counts on each node
   *
   * <p>Here is an example of assigning 10 partitions with 3 replicas each to 5 brokers:
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3rd replica)
   * p7        p8        p9        p5        p6       (3rd replica)
   *
   * <p>For tenant-aware partition assignment, we attempt to balance partitions of each tenant
   * across brokers. After allocating multiples of brokers, remaining leader replicas are allocated
   * based on lowest leader counts at tenant level (followed by total if tenant counts are equal)
   * and follower replicas are allocated based on lowest follower counts.
   */
  private List<List<Integer>> assignReplicasToBrokersRackUnaware(
      ClusterMetadata clusterMetadata,
      TopicInfo topicInfo,
      Map<Integer, ReplicaCounts> leaderCounts,
      Map<Integer, ReplicaCounts> followerCounts) {

    List<Integer> nodesOrderedByLeaders = orderNodes(leaderCounts);
    List<Integer> nodesOrderedByFollowers = orderNodes(followerCounts);
    return assignReplicasToBrokers(clusterMetadata,
                                   topicInfo,
                                   leaderCounts,
                                   followerCounts,
                                   nodesOrderedByLeaders,
                                   nodesOrderedByFollowers);
  }

  /**
   * Rack-aware, tenant-aware replica assignment.
   *
   * <p>The goals of this assignment:
   * 1. Spread partition replicas of a topic evenly among brokers.
   * 2. Spread replicas of a tenant evenly among brokers.
   * 3. Spread replicas evenly among brokers if possible (1. and 2. are attempted first).
   * 4. For partitions assigned to a particular broker, their other replicas are spread over
   *    the other brokers.
   * 5. Assign the replicas for each partition to different racks if possible
   *
   * <p>To create rack aware assignment, we first create a rack alternated broker list. For example,
   * from this brokerID -> rack mapping:
   * <code>
   * 0 -> "rack1", 1 -> "rack3", 2 -> "rack3", 3 -> "rack2", 4 -> "rack2", 5 -> "rack1"
   * </code>
   *
   * <p>The rack alternated list will be:
   * <code>
   * 0, 1, 3, 5, 2, 4 (rack1, rack3, rack2, rack1, rack3, rack2)
   * </code>
   *
   * <p>Then an easy round-robin assignment can be applied. Assume 6 partitions with
   * replication factor of 3, the assignment will be:
   * <code>
   * p0 -> 0, 1, 3
   * p1 -> 1, 3, 5
   * p2 -> 3, 5, 2
   * p3 -> 5, 2, 4
   * p4 -> 2, 4, 0
   * p5 -> 4, 0, 1
   * </code>
   *
   * <p>Once it has completed the first round-robin, if there are more partitions to assign, the
   * algorithm will start shifting the followers. This is to ensure we will not always get the same
   * set of sequences. In this case, if there are a couple more partitions to assign
   * (partition #6, #7), the assignment will be:
   * <code>
   * p6 -> 0, 3, 1
   * p7 -> 1, 0, 4
   * </code>
   *
   * <p>The rack aware assignment always chooses the 1st replica of the partition using round robin
   * on the rack alternated broker list. For rest of the replicas, it will be biased towards brokers
   * on racks that do not have any replica assignment, until every rack has a replica. Then the
   * assignment will go back to round-robin on the broker list.
   *
   * <p>As the result, if the number of replicas is equal to or greater than the number of racks,
   * it will ensure that each rack will get at least one replica. Otherwise, each rack will get
   * at most one replica. In a perfect situation where the number of replicas is the same as the
   * number of racks and each rack has the same number of brokers, it guarantees that the replica
   * distribution is even across brokers and racks.
   *
   * <p>For tenant-aware partition assignment, we attempt to balance partitions of each tenant
   * across brokers. After allocating multiples of brokers, remaining leader replicas are allocated
   * based on lowest leader counts at tenant level (followed by total if tenant counts are equal)
   * and follower replicas are allocated based on lowest follower counts.
   *
   */
  private List<List<Integer>> assignReplicasToBrokersRackAware(
      ClusterMetadata clusterMetadata,
      TopicInfo topicInfo,
      Map<Integer, ReplicaCounts> leaderCounts,
      final Map<Integer, ReplicaCounts> followerCounts) {

    List<Integer> nodesOrderedByLeaders = orderNodes(leaderCounts);
    nodesOrderedByLeaders = rackAlternatedBrokerList(nodesOrderedByLeaders,
                                                     clusterMetadata.brokerRackMap,
                                                     Collections.emptyList());
    List<String> leaderRackOrder = nodesOrderedByLeaders.stream()
        .map(clusterMetadata.brokerRackMap::get)
        .collect(Collectors.toList());

    List<Integer> nodesOrderedByFollowers = orderNodes(followerCounts);
    nodesOrderedByFollowers = rackAlternatedBrokerList(nodesOrderedByFollowers,
                                                      clusterMetadata.brokerRackMap,
                                                      leaderRackOrder);
    return assignReplicasToBrokers(clusterMetadata,
                                   topicInfo,
                                   leaderCounts,
                                   followerCounts,
                                   nodesOrderedByLeaders,
                                   nodesOrderedByFollowers);
  }

  /**
   * Generates rack alternate list of brokers. Within each rack, the ordering of nodes
   * in `orderedBrokers` is retained.
   *
   * @param orderedBrokers Ordered list of brokers (e.g. by leader count)
   * @param brokerRackMap racks associated with each node
   * @param disallowedOrder Avoid rack positions from this list,
   *                   e.g. leader racks when assigning followers
   * @return Rack alternate list of broker ids
   */
  static List<Integer> rackAlternatedBrokerList(List<Integer> orderedBrokers,
                                                Map<Integer, String> brokerRackMap,
                                                List<String> disallowedOrder) {
    List<String> rackList = new ArrayList<>();
    Map<String, List<Integer>> brokersByRack = new HashMap<>();
    for (Integer brokerId: orderedBrokers) {
      String rack = brokerRackMap.get(brokerId);
      if (!rackList.contains(rack)) {
        rackList.add(rack);
        brokersByRack.put(rack, new ArrayList<>());
      }
      brokersByRack.get(rack).add(brokerId);
    }

    int numRacks = rackList.size();
    List<String> rackOrder;
    if (disallowedOrder.isEmpty()) {
      rackOrder = rackList;
    } else {
      // When allocating followers, we want different rack assignment from leader.
      // So find a valid rack order where each slot has a different rack from the leader racks
      // provided in `disallowedOrder`
      rackOrder = new ArrayList<>(numRacks);
      for (int i = 0; i < numRacks; i++) {
        for (Iterator<String> it = rackList.iterator(); it.hasNext(); ) {
          String nextRack = it.next();
          if (!disallowedOrder.get(i).equals(nextRack)) {
            rackOrder.add(nextRack);
            it.remove();
            break;
          }
        }
      }
      if (!rackList.isEmpty()) { // Last one remaining is not valid for that index, swap
        rackOrder.add(rackOrder.size() - 1, rackList.get(0));
      }
    }

    // Create a rack alternate list of brokers using the rack ordering `rackOrder`
    Map<String, Iterator<Integer>> brokersIteratorByRack = brokersByRack.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().iterator()));

    List<Integer> result = new ArrayList<>();
    int rackIndex = 0;
    while (result.size() < orderedBrokers.size()) {
      Iterator<Integer> rackIterator = brokersIteratorByRack.get(rackOrder.get(rackIndex));
      if (rackIterator.hasNext()) {
        result.add(rackIterator.next());
      }
      rackIndex = (rackIndex + 1) % numRacks;
    }
    return result;
  }

  private List<List<Integer>> assignReplicasToBrokers(ClusterMetadata clusterMetadata,
                                                      TopicInfo topicInfo,
                                                      Map<Integer, ReplicaCounts> leaderCounts,
                                                      Map<Integer, ReplicaCounts> followerCounts,
                                                      List<Integer> nodesOrderedByLeaders,
                                                      List<Integer> nodesOrderedByFollowers) {

    short replicationFactor = topicInfo.replicationFactor;
    int startPartitionId = topicInfo.firstNewPartition;
    int numNewPartitions = topicInfo.totalPartitions - startPartitionId;

    // Round-robin allocation of leaders
    List<List<Integer>> assignment =
        allocateLeaders(numNewPartitions, replicationFactor, leaderCounts, nodesOrderedByLeaders);

    // Allocate followers in batches of `numBrokers`. First batch contains the remainder if number
    // of partitions being allocated is not a multiple of `numBrokers`. Least loaded nodes by
    // follower count are assigned for the remainder to reduce any imbalance.
    int numBrokers = leaderCounts.size();
    int remainder = numNewPartitions % leaderCounts.size();
    for (int batch = 0, p = 0; p < numNewPartitions; batch++) {
      int batchSize = batch == 0 && remainder != 0 ? remainder : numBrokers;
      allocateFollowers(clusterMetadata,
                        p,
                        batchSize,
                        replicationFactor,
                        batch,
                        followerCounts,
                        assignment,
                        nodesOrderedByFollowers);
      p += batchSize;
    }
    return assignment;
  }

  /**
   * Performs round robin allocation of leaders and returns the resulting assignment.
   * Allocation is performed using a broker list ordered by leader counts.
   */
  private List<List<Integer>> allocateLeaders(int numNewPartitions,
                                              short replicationFactor,
                                              Map<Integer, ReplicaCounts> leaderCounts,
                                              List<Integer> orderedBrokerList) {
    List<List<Integer>> assignment = new ArrayList<>(numNewPartitions);

    int numBrokers = leaderCounts.size();
    for (int p = 0; p < numNewPartitions; p++) {
      int leaderId = orderedBrokerList.get(p % numBrokers);
      List<Integer> replicas = new ArrayList<>(replicationFactor);
      replicas.add(leaderId);
      assignment.add(replicas);
      leaderCounts.get(leaderId).incrementCounts();
    }
    return assignment;
  }

  /**
   * Performs allocation of followers and updates the provided `assignment`.
   */
  private void allocateFollowers(ClusterMetadata clusterMetadata,
                                 int firstPartitionIndex,
                                 int numPartitions,
                                 int replicationFactor,
                                 int batchIndex,
                                 Map<Integer, ReplicaCounts> followerCounts,
                                 List<List<Integer>> assignment,
                                 List<Integer> orderedBrokerList) {

    int numBrokers = clusterMetadata.brokers.size();

    List<Integer> brokerList;
    int replicaShift;
    if (numPartitions < numBrokers) {
      // Choose the least loaded nodes that can be used to create a valid assignment
      brokerList = orderedBrokerList;
      replicaShift = 0;
    } else {
      // Use the same ordering as leader brokers to ensure increasing shift generates a
      // balanced distribution with differing assignments
      brokerList =
          assignment.subList(firstPartitionIndex, firstPartitionIndex + numPartitions)
              .stream().map(l -> l.get(0)).collect(Collectors.toList());
      int numRacks = clusterMetadata.racks.isEmpty() ? 1 : clusterMetadata.racks.size();
      replicaShift = (batchIndex * numRacks) % (numBrokers - 1) + 1;
    }

    List<Integer> prevUnassignedBrokers = Collections.emptyList();
    for (int r = 0; r < replicationFactor - 1; r++) {
      List<Integer> unassignedBrokers = rotateList(brokerList, replicaShift);
      unassignedBrokers.removeAll(prevUnassignedBrokers);
      unassignedBrokers.addAll(0, prevUnassignedBrokers);

      for (int p = 0; p < numPartitions; p++) {
        List<Integer> assignedReplicas = assignment.get(p + firstPartitionIndex);

        // We attempt to allocate the first valid replica from `assignedBrokers`. If there aren't
        // any valid assignments with these, we allocate any broker that can create a valid
        // assignment. This could create a slight imbalance in this assignment, but this should be
        // ok since we prefer least loaded replicas in subsequent assignments.
        Integer broker = maybeAssign(clusterMetadata, unassignedBrokers, assignedReplicas);
        if (broker != null) {
          unassignedBrokers.remove(broker);
        } else {
          List<Integer> orderedList = orderNodes(followerCounts);
          broker = maybeAssign(clusterMetadata, orderedList, assignedReplicas);
          if (broker == null) {
            throw new IllegalStateException("No valid assignment found");
          }
        }
        followerCounts.get(broker).incrementCounts();
      }
      replicaShift = numPartitions == brokerList.size() ? replicaShift + 1 : numPartitions;
      replicaShift = (replicaShift % numBrokers == 0) ? 1 : replicaShift % numBrokers;
      prevUnassignedBrokers = unassignedBrokers;
    }
  }

  /**
   * Add an assignment to `assignedReplicas` if a valid assignment can be performed
   * using one of the brokers from `brokerList`.
   */
  private Integer maybeAssign(ClusterMetadata clusterMetadata,
                              List<Integer> brokerList,
                              List<Integer> assignedReplicas) {
    for (int k = 0; k < brokerList.size(); k++) {
      int broker = brokerList.get(k);
      if (validReplicaAssignment(clusterMetadata, broker, assignedReplicas)) {
        assignedReplicas.add(broker);
        return broker;
      }
    }
    return null;
  }

  // Replica is not valid if any of these conditions is true:
  // 1. a replica for this partition has already been assigned to the broker
  // 2. there is already a broker in the same rack that has assigned a replica AND
  //    there is one or more rack that do not have any replica
  private boolean validReplicaAssignment(ClusterMetadata clusterMetadata,
                                         int broker,
                                         List<Integer> assignedReplicas) {
    int numRacks = clusterMetadata.racks.size();
    if (assignedReplicas.contains(broker)) {
      return false;
    } else if (numRacks <= 1) {
      return true;
    } else {
      String rack = clusterMetadata.brokerRackMap.get(broker);
      Set<String> assignedRacks = assignedReplicas.stream().map(clusterMetadata.brokerRackMap::get)
          .collect(Collectors.toSet());
      return !assignedRacks.contains(rack) || assignedRacks.size() == numRacks;
    }
  }

  /**
   * Returns node ids ordered by replica counts. Three levels of comparison are used:
   *   1) Lower number of topic replicas
   *   2) Lower number of tenant replicas
   *   3) Lower number of total replicas
   */
  static List<Integer> orderNodes(final Map<Integer, ReplicaCounts> nodeReplicaCounts) {
    List<Integer> orderedNodes = new ArrayList<>(nodeReplicaCounts.keySet());
    orderedNodes.sort(Comparator.comparing(nodeReplicaCounts::get));
    return orderedNodes;
  }

  /**
   * Returns a new list that rotates `list` by the provided `shift` entries.
   */
  private static List<Integer> rotateList(List<Integer> list, int shift) {
    int count = list.size();
    List<Integer> result = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      result.add(list.get((i + shift) % count));
    }
    return result;
  }

  /**
   * Topic details from new topic or partition create request.
   */
  public static class TopicInfo {
    final int totalPartitions;
    final short replicationFactor;
    final int firstNewPartition;

    public TopicInfo(int totalPartitions, short replicationFactor, int firstNewPartition) {
      this.totalPartitions = totalPartitions;
      this.replicationFactor = replicationFactor;
      this.firstNewPartition = firstNewPartition;
    }

    public int totalPartitions() {
      return totalPartitions;
    }

    public short replicationFactor() {
      return replicationFactor;
    }

    public boolean isNewTopic() {
      return firstNewPartition == 0;
    }
  }
}
