// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.integration.cluster;

public class UserMetadata {

  private final int userId;
  private final String apiKey;
  private final String apiSecret;
  private final boolean isSuperUser;

  public UserMetadata(int userId, String apiKey, String apiSecret, boolean isSuperUser) {
    this.userId = userId;
    this.apiKey = apiKey;
    this.apiSecret = apiSecret;
    this.isSuperUser = isSuperUser;
  }

  public int userId() {
    return userId;
  }

  public String apiKey() {
    return apiKey;
  }

  public String apiSecret() {
    return apiSecret;
  }

  public boolean isSuperUser() {
    return isSuperUser;
  }

  @Override
  public String toString() {
    String type = isSuperUser ? "SuperUser" : "Service";
    return String.format("%s:id=%d,apiKey=%s", type, userId, apiKey);
  }

}
