// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.ldap;

import org.apache.kafka.common.KafkaException;

public class LdapException extends KafkaException {

  private static final long serialVersionUID = 1L;

  public LdapException(String message) {
    super(message);
  }

  public LdapException(String message, Throwable cause) {
    super(message, cause);
  }

}
