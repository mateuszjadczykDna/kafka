// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth.stats;

public interface AuthenticationStatsMBean {

  long getTotal();

  long getSucceeded();

  long getFailed();
}
