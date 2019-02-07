// (Copyright) [2017 - 2019] Confluent, Inc.

package io.confluent.kafka.common.multitenant.oauth;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import java.util.List;
import java.util.Set;

public class OAuthBearerJwsToken implements OAuthBearerToken {

  public static final String OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY = "logicalCluster";

  private final String jwtId;
  private final String value;
  private final String principalName;
  private final Set<String> scope;
  private final List<String> allowedClusters;
  private final long lifetimeMs;
  private final Long startTimeMs;

  public OAuthBearerJwsToken(String value, Set<String> scope, long lifetimeMs,
                             String principalName, Long startTimeMs, String jwtId,
                             List<String> allowedClusters) {
    this.value = value;
    this.principalName = principalName;
    this.scope = scope;
    this.lifetimeMs = lifetimeMs;
    this.startTimeMs = startTimeMs;
    this.allowedClusters = allowedClusters;
    this.jwtId = jwtId;
  }

  @Override
  public String value() {
    return value;
  }

  @Override
  public Set<String> scope() {
    return scope;
  }

  @Override
  public long lifetimeMs() {
    return lifetimeMs;
  }

  @Override
  public String principalName() {
    return principalName;
  }

  @Override
  public Long startTimeMs() {
    return startTimeMs;
  }

  public List<String> allowedClusters() {
    return allowedClusters;
  }

  public String jwtId() {
    return jwtId;
  }
}
