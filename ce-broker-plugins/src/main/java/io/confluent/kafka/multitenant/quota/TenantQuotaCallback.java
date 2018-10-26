// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.quota;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.multitenant.metrics.TenantMetrics;
import io.confluent.kafka.multitenant.schema.TenantContext;
import java.util.Objects;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TenantQuotaCallback implements ClientQuotaCallback {
  private static final Logger logger = LoggerFactory.getLogger(TenantQuotaCallback.class);

  static final String MIN_PARTITIONS_CONFIG = "confluent.quota.min.partitions";
  public static final int DEFAULT_MIN_PARTITIONS = 8;

  // TODO: This is a temporary workaround to track TenantQuotaCallbacks.
  // This is used by interceptors to find a partition assignor that has access to
  // the cluster metadata from the configured quota callback. This is also used
  // by cloud secret file loader to notify quota callback of updates to tenant's
  // quotas whenever the file is loaded.
  private static final Map<Integer, TenantQuotaCallback> INSTANCES = new HashMap<>();

  private final EnumMap<ClientQuotaType, AtomicBoolean> quotaResetPending =
      new EnumMap<>(ClientQuotaType.class);
  private final ConcurrentHashMap<String, TenantQuota> tenantQuotas;
  private final TenantPartitionAssignor partitionAssignor;

  private volatile int brokerId;
  private volatile int minPartitionsForMaxQuota;
  private volatile Cluster cluster;
  private volatile QuotaConfig defaultTenantQuota;

  public TenantQuotaCallback() {
    for (ClientQuotaType quotaType : ClientQuotaType.values()) {
      quotaResetPending.put(quotaType, new AtomicBoolean());
    }
    tenantQuotas = new ConcurrentHashMap<>();
    this.defaultTenantQuota = QuotaConfig.UNLIMITED_QUOTA;
    this.partitionAssignor = new TenantPartitionAssignor();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.brokerId = intConfig(configs, KafkaConfig$.MODULE$.BrokerIdProp(), null);
    synchronized (INSTANCES) {
      INSTANCES.put(brokerId, this);
    }

    this.minPartitionsForMaxQuota = intConfig(configs, MIN_PARTITIONS_CONFIG,
        DEFAULT_MIN_PARTITIONS);
    if (minPartitionsForMaxQuota < 1) {
      throw new ConfigException(MIN_PARTITIONS_CONFIG + " must be >= 1, but got "
          + minPartitionsForMaxQuota);
    }
    logger.info("Configured tenant quota callback for broker {} with {}={}",
        brokerId, MIN_PARTITIONS_CONFIG, minPartitionsForMaxQuota);
  }

  @Override
  public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal,
                                             String clientId) {
    if (principal instanceof MultiTenantPrincipal) {
      TenantMetadata tenantMetadata = ((MultiTenantPrincipal) principal).tenantMetadata();
      String tenant = tenantMetadata.tenantName;
      TenantQuota tenantQuota = getOrCreateTenantQuota(tenant, defaultTenantQuota, false);
      if (!tenantQuota.hasQuotaLimit(quotaType)) {
        // Unlimited quota configured for tenant/default, so not adding any tags
        return Collections.emptyMap();
      } else {
        // We currently have only one-level of quotas for tenants, so it is safe to return
        // just the tenant tags.
        return tenantMetricTags(((MultiTenantPrincipal) principal).tenantMetadata().tenantName);
      }
    } else {
      // For internal listeners, the principal will not be a tenant principal and
      // we don't currently configure quotas for clients on these listeners.
      return Collections.emptyMap();
    }
  }

  @Override
  public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
    String tenant = metricTags.get(TenantMetrics.TENANT_TAG);
    if (tenant == null || tenant.isEmpty()) {
      return QuotaConfig.UNLIMITED_QUOTA.quota(quotaType);
    } else {
      TenantQuota tenantQuota = tenantQuotas.get(tenant);
      if (tenantQuota != null) {
        return tenantQuota.quotaLimit(quotaType);
      } else {
        logger.warn("Quota not found for tenant {}, using default quota", tenant);
        return defaultTenantQuota.quota(quotaType);
      }
    }
  }

  @Override
  public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity,
                          double newValue) {
    // We currently don't use quotas configured in ZooKeeper
  }

  @Override
  public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
    // We currently don't use quotas configured in ZooKeeper
  }

  @Override
  public boolean quotaResetRequired(ClientQuotaType quotaType) {
    return quotaResetPending.get(quotaType).getAndSet(false);
  }

  /**
   * Handle metadata update. This method is invoked whenever the broker receives
   * UpdateMetadata request from the controller. Recompute all quotas to take
   * the current partition allocation into account.
   */
  @Override
  public synchronized boolean updateClusterMetadata(Cluster cluster) {
    logger.debug("Updating cluster metadata {}", cluster);
    partitionAssignor.updateClusterMetadata(cluster);

    this.cluster = cluster;
    Map<String, Integer> totalTenantPartitions = new HashMap<>();
    Map<String, Integer> tenantPartitionsOnThisBroker = new HashMap<>();
    tenantQuotas.keySet().forEach(tenant -> totalTenantPartitions.put(tenant, 0));
    for (String topic : cluster.topics()) {
      String tenant = topicTenant(topic);
      if (!tenant.isEmpty()) {
        for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
          totalTenantPartitions.merge(tenant, 1, Integer::sum);
          Node leader = partitionInfo.leader();
          if (leader != null && leader.id() == brokerId) {
            tenantPartitionsOnThisBroker.merge(tenant, 1, Integer::sum);
          }
        }
      }
    }
    boolean updated = false;
    for (Map.Entry<String, Integer> entry : totalTenantPartitions.entrySet()) {
      String tenant = entry.getKey();
      TenantQuota tenantQuota = getOrCreateTenantQuota(tenant, defaultTenantQuota, false);
      int total = entry.getValue();
      Integer partitionsOnThisBroker = tenantPartitionsOnThisBroker.getOrDefault(tenant, 0);
      updated |= tenantQuota.updatePartitions(partitionsOnThisBroker, total);
    }
    if (updated) {
      logger.trace("Some tenant quotas have been updated, new quotas: {}", tenantQuotas);
    }
    return updated;
  }

  public Cluster cluster() {
    return cluster;
  }

  @Override
  public void close() {
    synchronized (INSTANCES) {
      INSTANCES.remove(brokerId);
    }
  }

  private TenantQuota getOrCreateTenantQuota(String tenant,
                                             QuotaConfig clusterQuotaConfig,
                                             boolean forceUpdate) {
    boolean created = false;
    TenantQuota tenantQuota = new TenantQuota();
    TenantQuota prevQuota = tenantQuotas.putIfAbsent(tenant, tenantQuota);
    if (prevQuota != null) {
      tenantQuota = prevQuota;
    } else {
      created = true;
    }
    if (created || forceUpdate) {
      tenantQuota.updateClusterQuota(clusterQuotaConfig);
    }
    return tenantQuota;
  }

  /**
   * Update provisioned tenant quota configuration. This method is invoked when tenant
   * cluster quotas or default tenant cluster quota is updated. Recompute quotas for
   * all affected tenants.
   */
  private synchronized void updateTenantQuotas(Map<String, QuotaConfig> tenantClusterQuotas,
      QuotaConfig defaultTenantQuota) {
    this.defaultTenantQuota = defaultTenantQuota;
    tenantQuotas.keySet().removeIf(tenant -> !tenantClusterQuotas.containsKey(tenant));
    for (Map.Entry<String, QuotaConfig> entry : tenantClusterQuotas.entrySet()) {
      getOrCreateTenantQuota(entry.getKey(), entry.getValue(), true);
    }
    logger.trace("Updated tenant quotas, new quotas: {}", tenantQuotas);
  }

  /**
   * Update provisioned tenant quota configuration and/or default tenant quota.
   * This method is invoked when tenant cluster quotas or default tenant cluster quota is updated.
   */
  public static void updateQuotas(Map<String, QuotaConfig> tenantQuotas,
                                  QuotaConfig defaultTenantQuota) {
    logger.debug("Update quotas: tenantQuotas={} default={}", tenantQuotas, defaultTenantQuota);
    synchronized (INSTANCES) {
      INSTANCES.values()
          .forEach(callback -> callback.updateTenantQuotas(tenantQuotas, defaultTenantQuota));
    }
  }

  public static TenantPartitionAssignor partitionAssignor(Map<String, ?> configs) {
    int brokerId = intConfig(configs, KafkaConfig$.MODULE$.BrokerIdProp(), null);
    TenantPartitionAssignor partitionAssignor = null;
    synchronized (INSTANCES) {
      TenantQuotaCallback quotaCallback = INSTANCES.get(brokerId);
      if (quotaCallback != null) {
        partitionAssignor = quotaCallback.partitionAssignor;
      } else {
        logger.debug("Tenant quota callback not configured for broker {}", brokerId);
      }
    }
    return partitionAssignor;
  }

  // Used only in tests
  static void closeAll() {
    synchronized (INSTANCES) {
      while (!INSTANCES.isEmpty()) {
        INSTANCES.values().iterator().next().close();
      }
    }
  }

  private static String topicTenant(String topic) {
    if (TenantContext.isTenantPrefixed(topic)) {
      return TenantContext.extractTenant(topic);
    } else {
      return "";
    }
  }

  private static Map<String, String> tenantMetricTags(String tenant) {
    return Collections.singletonMap(TenantMetrics.TENANT_TAG, tenant);
  }

  private static int intConfig(Map<String, ?> configs, String configName, Integer defaultValue) {
    Object configValue = configs.get(configName);
    if (configValue == null && defaultValue != null) {
      return defaultValue;
    }
    if (configValue == null) {
      throw new ConfigException(configName + " is not set");
    }
    return Integer.parseInt(configValue.toString());
  }

  private class TenantQuota {
    // Cluster configs related to the tenant, accessed only with TenantQuotaCallback lock
    int leaderPartitions;   // Tenant partitions with this broker as leader
    int totalPartitions;    // Total number of tenant partitions
    // Configured cluster-wide quota
    QuotaConfig clusterQuotaConfig;

    // Quotas for this broker
    volatile QuotaConfig brokerQuotas;

    /**
     * Recomputes tenant quota for this broker based on the provided leader partitions of
     * this tenant on this broker and the total number of tenant partitions.
     */
    boolean updatePartitions(int leaderPartitions, int totalPartitions) {
      this.leaderPartitions = leaderPartitions;
      this.totalPartitions = totalPartitions;
      QuotaConfig oldBrokerQuotas = brokerQuotas;
      updateBrokerQuota();
      return !Objects.equals(oldBrokerQuotas, brokerQuotas);
    }

    /**
     * Recomputes tenant quota for this broker based on the new provisioned cluster quota config
     * provided.
     */
    void updateClusterQuota(QuotaConfig clusterQuotaConfig) {
      if (!clusterQuotaConfig.equals(this.clusterQuotaConfig)) {
        this.clusterQuotaConfig = clusterQuotaConfig;
        updateBrokerQuota();
      }
    }

    /**
     * Updates the quotas for this broker based on the configured provisioned cluster quota
     * for the tenant and the proportion of tenant partitions allocated to this broker (as leader).
     * If the provisioned cluster quota is not yet known (e.g. tenant quota has not yet been
     * refreshed on the broker), default tenant quota is used for the calculation. Quota returned
     * is always greater than zero to avoid excessive throttling of requests received before
     * cluster metadata or quota configs are refreshed.
     *
     * <p>For a cluster with:
     *    nodes = `n`,
     *    tenant cluster quota = `c`,
     *    total tenant partitions = `p`,
     *    tenant partitions with this broker as leader = `l`,
     *    confluent.quota.min.partitions = `m`
     *    default tenant cluster quota = `d`
     * Tenant quota 'q` is computed as follows:
     *    q = l == 0 ? c / max(p, m, n) : c * l / max(p, m)
     * </p><p>Scenarios:
     * <ul>
     *   <li>Typical case - quotas divided proportionally to leaders, full quota achieved across
     *       cluster:
     *       l > 0, p > 0, n > 0, p > m  : q = c * l / p
     *   </li>
     *   <li>Tenant creates one (or a small number) of partitions, and this broker is the leader
     *       of one or more partitions. Since tenant partitions will be balanced, we expect this
     *       broker to be the leader of at most one partition. But for flexibility of data
     *       rebalancing, we allow l > 1. Full quota achieved if m == 1 or p >= m.
     *       l > 0, p < n, n > 0 : q = c * l / max(p, m)
     *   </li>
     *   <li>Tenant created, metadata not refreshed on this broker - quota divided equally
     *       amongst nodes until partitions are created and metadata is refreshed.
     *       l = 0, p = 0, n > 0 : q = c / max(m, n)
     *   </li>
     *   <li>Tenant creates partitions across all brokers, but this broker is not currently
     *       the leader of any. To avoid excessive throttling if a request arrives before metadata
     *       is refreshed, quota for one partition is allocated to this broker. This is a very
     *       tiny timing window, so the additional quota shouldn't cause any issues.
     *       l = 0, p > n, n > 0 : q = c / max(p, m, n)
     *   </li>
     *   <li>Tenant creates one (or a small number) of partitions, but this broker is not currently
     *       the leader of any. Partitions are balanced across brokers, so quota is divided
     *       equally amongst brokers, including for this broker which is not the leader of any.
     *       l = 0, p < n, n > 0 : q = c / max(m, n)
     *   </li>
     *   <li>Tenant request arrives before tenant quota is refreshed on the callback.
     *       `q` is calculated with cluster quota `d` based on one of the scenarios above.
     *   </li>
     *   <li>Tenant request arrives before cluster metadata is refreshed on the callback.
     *       `q` is calculated with number of nodes from previous cluster metadata if available or
     *       `m` as the number of nodes, based on one of the scenarios above.
     *   </li>
     * </ul>
     * </p>
     * <p>Quota guarantees:
     * <ul>
     * <li>We always guarantee quota > 0 to avoid excessive throttling.</li>
     * <li>For small timing windows related to metadata refresh, the total cluster quota
     *     allocated across brokers may be higher than the total provisioned quota, but
     *     is expected to be adjusted very quickly since this window only appears due
     *     to request handling on different threads</li>
     * <li>For larger timing windows related to tenant quota refresh, we may allocate
     *     older or default tenant quotas until the refresh is processed, but we will not
     *     exceed the total (older/default) quota of the tenant across the cluster.</li>
     * <li>The maximum quota achievable per-partition will be `c / m`. But quota guarantees
     *     will be on the total of all partitions on each broker and not at individual
     *     partitions level. At least `m` partitions are required to achieve the provisioned
     *     quota across the cluster.</li>
     * <li>Brokers (with non-tenant principals) are allocated unlimited quotas and are never
     *     throttled.</li>
     * </ul>
     * </p>
     */
    void updateBrokerQuota() {
      int denominator = Math.max(totalPartitions, minPartitionsForMaxQuota);
      double numerator = Math.max(leaderPartitions, 1.0);
      if (leaderPartitions == 0) {
        int numBrokers = cluster == null ? 1 : cluster.nodes().size();
        denominator = Math.max(denominator, numBrokers);
      }
      double multiplier =  numerator / denominator;

      // TODO: More investigation is required to figure out the best way to
      // distribute request quota. In phase 1, we will allocate high request
      // quotas to reduce throttling based on request quotas.
      brokerQuotas = new QuotaConfig(clusterQuotaConfig, multiplier);
    }

    boolean hasQuotaLimit(ClientQuotaType quotaType) {
      return brokerQuotas.hasQuotaLimit(quotaType);
    }

    Double quotaLimit(ClientQuotaType quotaType) {
      return brokerQuotas.quota(quotaType);
    }

    @Override
    public String toString() {
      return "TenantQuota("
          + "totalPartitions=" + totalPartitions + ", "
          + "leaderPartitions=" + leaderPartitions + ", "
          + "clusterQuotaConfig=" + clusterQuotaConfig + ", "
          + "brokerQuotas=" + brokerQuotas + ")";
    }
  }
}

