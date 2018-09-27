// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.interceptor.BrokerInterceptor;

import io.confluent.kafka.multitenant.metrics.TenantMetrics;

import java.net.InetAddress;
import java.util.Map;

public class MultiTenantInterceptor implements BrokerInterceptor {

  private final Time time;
  private final TenantMetrics tenantMetrics;

  public MultiTenantInterceptor() {
    this.time = Time.SYSTEM;
    this.tenantMetrics = new TenantMetrics();
  }

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public RequestContext newContext(RequestHeader header,
                                   String connectionId,
                                   InetAddress clientAddress,
                                   KafkaPrincipal principal,
                                   ListenerName listenerName,
                                   SecurityProtocol securityProtocol,
                                   Metrics metrics) {
    return new MultiTenantRequestContext(header, connectionId, clientAddress, principal,
        listenerName, securityProtocol, time, metrics, tenantMetrics);
  }

}
