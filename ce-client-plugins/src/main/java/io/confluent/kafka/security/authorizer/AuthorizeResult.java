// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

public enum AuthorizeResult {
  ALLOWED,
  DENIED,
  UNKNOWN_SCOPE,
  AUTHORIZER_FAILED,
  UNKNOWN_ERROR
}