// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.provider;

import java.io.Closeable;
import java.util.Set;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Interface used by providers of user to group mapping used for authorization.
 */
public interface GroupProvider extends Configurable, Closeable {

  /**
   * Returns the groups of the provided user principal.
   * @param sessionPrincipal User principal of the Session.
   * @return Set of group principals of the user, which may be empty.
   */
  Set<KafkaPrincipal> groups(KafkaPrincipal sessionPrincipal);

  /**
   * Returns the name of this provider.
   * @return provider name
   */
  String providerName();
}
