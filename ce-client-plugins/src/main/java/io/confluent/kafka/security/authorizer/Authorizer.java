// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import java.io.Closeable;
import java.util.List;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Cross-component authorizer API.
 */
public interface Authorizer extends Configurable, Closeable {
  List<AuthorizeResult> authorize(KafkaPrincipal sessionPrincipal, String host, List<Action> actions);
}
