// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

public class MetadataServiceConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;
  private static final String METADATA_SERVER_PREFIX = "confluent.metadata.server.";

  public static final String SCOPE_PROP = "confluent.metadata.server.scope";
  private static final String SCOPE_DOC = "The root scope of the metadata server."
      + " This must be non-empty if RBAC metadata provider is enabled.";

  public static final String METADATA_SERVER_CLASS_PROP = "confluent.metadata.server.class";
  private static final String METADATA_SERVER_CLASS_DOC = "The fully qualified name of a class that implements"
      + " the " + MetadataServer.class + " interface. Additional configs for the metadata service may be"
      + " provided with the prefix '" + METADATA_SERVER_PREFIX + "'.";

  static {
    CONFIG = new ConfigDef()
        .define(METADATA_SERVER_CLASS_PROP, Type.CLASS, Importance.HIGH, METADATA_SERVER_CLASS_DOC)
        .define(SCOPE_PROP, Type.STRING, Importance.HIGH, SCOPE_DOC);
  }

  public final String scope;
  public final MetadataServer metadataServer;

  @SuppressWarnings("unchecked")
  public MetadataServiceConfig(Map<?, ?> props) {
    super(CONFIG, props);
    scope = getString(SCOPE_PROP);
    if (scope == null || scope.isEmpty())
      throw new ConfigException("Scope for metadata server must be non-empty");

    Class<MetadataServer> metadataServerClass = (Class<MetadataServer>) getClass(METADATA_SERVER_CLASS_PROP);
    if (metadataServerClass != null) {
      this.metadataServer = getConfiguredInstances(
          Collections.singletonList(metadataServerClass.getName()),
          MetadataServer.class, metadataServerConfigs()).get(0);
    } else
      throw new ConfigException("Metadata server class not provided");
  }

  public Map<String, Object> metadataServerConfigs() {
    return originalsWithPrefix(METADATA_SERVER_PREFIX);
  }

  @Override
  public String toString() {
    return Utils.mkString(values(), "", "", "=", "%n\t");
  }

  public static void main(String[] args) throws Exception {
    try (PrintStream out = args.length == 0 ? System.out
        : new PrintStream(new FileOutputStream(args[0]), false, StandardCharsets.UTF_8.name())) {
      out.println(CONFIG.toHtmlTable());
      if (out != System.out) {
        out.close();
      }
    }
  }
}
