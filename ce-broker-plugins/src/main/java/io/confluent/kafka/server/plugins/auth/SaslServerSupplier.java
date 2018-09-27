// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.sasl.SaslServer;
import java.util.List;

public interface SaslServerSupplier {

  SaslServer get(List<AppConfigurationEntry> jaasContextEntries);

}
