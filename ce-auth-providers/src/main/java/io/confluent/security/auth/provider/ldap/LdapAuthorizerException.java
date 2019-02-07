// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.ldap;

import org.apache.kafka.common.KafkaException;

public class LdapAuthorizerException extends KafkaException {

  private static final long serialVersionUID = 1L;

  public LdapAuthorizerException(String message) {
    super(message);
  }

  public LdapAuthorizerException(String message, Throwable cause) {
    super(message, cause);
  }

}
