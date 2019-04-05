// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import java.io.Closeable;
import org.apache.kafka.common.Configurable;

/**
 * Password verifier for comparing passwords during authentication. Default password
 * verifier performs a simple string match. Custom password verifiers may support comparison
 * of encrypted passwords.
 */
public interface PasswordVerifier extends Configurable, Closeable {

  enum Result {
    MATCH,    // authentication succeeded
    MISMATCH, // authentication failed
    UNKNOWN   // failed to compare, e.g. due to unsupported encryption type. Other available password verifiers are checked
  }

  Result verify(char[] expectedPassword, char[] actualPassword);
}
