// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import java.security.Provider;
import java.security.Security;

public class FileBasedSaslServerProvider extends Provider {
  protected FileBasedSaslServerProvider() {
    super("File-based SASL/PLAIN Server Provider", 1.0,
            "Simple SASL/PLAIN Server Provider for Kafka");
    super.put("SaslServerFactory." + PlainSaslServer.PLAIN_MECHANISM,
              FileBasedSaslServerFactory.class.getName());
  }

  public static void initialize() {
    Security.insertProviderAt(new FileBasedSaslServerProvider(), 1);
  }
}
