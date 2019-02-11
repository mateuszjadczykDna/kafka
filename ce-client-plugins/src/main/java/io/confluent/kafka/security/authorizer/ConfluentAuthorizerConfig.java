// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders;
import io.confluent.kafka.security.authorizer.provider.GroupProvider;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.kafka.security.authorizer.provider.ConfluentBuiltInProviders.GroupProviders;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

public class ConfluentAuthorizerConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String SCOPE_PROP = "confluent.authorizer.scope";
  private static final String SCOPE_DEFAULT = "";
  private static final String SCOPE_DOC = "The root scope of this authorizer."
      + " This may be empty if RBAC provider is not enabled.";

  public static final String GROUP_PROVIDER_PROP = "confluent.authorizer.group.provider";
  private static final String GROUP_PROVIDER_DEFAULT = GroupProviders.NONE.name();
  private static final String GROUP_PROVIDER_DOC = "Group provider for the authorizer to map users to groups. "
      + " Supported providers are " + ConfluentBuiltInProviders.builtInGroupProviders()
      + ". Group-based authorization is disabled by default.";

  public static final String ACCESS_RULE_PROVIDERS_PROP = "confluent.authorizer.access.rule.providers";
  private static final String ACCESS_RULE_PROVIDERS_DEFAULT = AccessRuleProviders.ACL.name();
  private static final String ACCESS_RULE_PROVIDERS_DOC = "List of access rule providers enabled. "
      + " Access rule providers supported are " + ConfluentBuiltInProviders.builtInAccessRuleProviders()
      + ". ACL-based provider is enabled by default.";

  public static final String LICENSE_PROP = "confluent.license";
  private static final String LICENSE_DEFAULT = "";
  private static final String LICENSE_DOC = "License for Confluent plugins.";


  // SimpleAclAuthorizer configs

  public static final String ALLOW_IF_NO_ACLS_PROP = "allow.everyone.if.no.acl.found";
  private static final boolean ALLOW_IF_NO_ACLS_DEFAULT = false;
  private static final String ALLOW_IF_NO_ACLS_DOC =
      "Boolean flag that indicates if everyone is allowed access to a resource if no ACL is found.";

  public static final String SUPER_USERS_PROP = "super.users";
  private static final String SUPER_USERS_DEFAULT = "";
  private static final String SUPER_USERS_DOC = "Semicolon-separated list of principals of"
      + " super users who are allowed access to all resources.";

  static {
    CONFIG = new ConfigDef()
        .define(SCOPE_PROP, Type.STRING, SCOPE_DEFAULT,
            Importance.HIGH, SCOPE_DOC)
        .define(ALLOW_IF_NO_ACLS_PROP, Type.BOOLEAN, ALLOW_IF_NO_ACLS_DEFAULT,
            Importance.MEDIUM, ALLOW_IF_NO_ACLS_DOC)
        .define(SUPER_USERS_PROP, Type.STRING, SUPER_USERS_DEFAULT,
            Importance.MEDIUM, SUPER_USERS_DOC)
        .define(ACCESS_RULE_PROVIDERS_PROP, Type.LIST, ACCESS_RULE_PROVIDERS_DEFAULT,
            Importance.MEDIUM, ACCESS_RULE_PROVIDERS_DOC)
        .define(GROUP_PROVIDER_PROP, Type.LIST, GROUP_PROVIDER_DEFAULT,
            Importance.MEDIUM, GROUP_PROVIDER_DOC)
        .define(LICENSE_PROP, Type.STRING, LICENSE_DEFAULT,
            Importance.HIGH, LICENSE_DOC);
  }
  public final boolean allowEveryoneIfNoAcl;
  public final List<AccessRuleProvider> accessRuleProviders;
  public final GroupProvider groupProvider;
  public final String scope;

  public ConfluentAuthorizerConfig(Map<?, ?> props) {
    super(CONFIG, props);

    scope = getString(SCOPE_PROP);
    allowEveryoneIfNoAcl = getBoolean(ALLOW_IF_NO_ACLS_PROP);

    List<String> authProviderNames = getList(ACCESS_RULE_PROVIDERS_PROP);
    // Multitenant ACLs are included in the MultiTenantProvider, so include only the MultiTenantProvider
    if (authProviderNames.contains(AccessRuleProviders.ACL.name())
        && authProviderNames.contains(AccessRuleProviders.MULTI_TENANT.name())) {
      authProviderNames = new ArrayList<>(authProviderNames);
      authProviderNames.remove(AccessRuleProviders.ACL.name());
    }
    if (authProviderNames.isEmpty())
      throw new ConfigException("No authorization providers specified");

    accessRuleProviders = ConfluentBuiltInProviders.loadAccessRuleProviders(authProviderNames);
    accessRuleProviders.forEach(provider -> provider.configure(originals()));

    String groupFeature = (String) props.get(GROUP_PROVIDER_PROP);
    String groupProviderName = groupFeature == null || groupFeature.isEmpty()
        ? GroupProviders.NONE.name() : groupFeature;
    Optional<AccessRuleProvider> combinedProvider = accessRuleProviders.stream()
        .filter(p -> p.providerName().equals(groupProviderName) && p instanceof GroupProvider)
        .findFirst();
    if (combinedProvider.isPresent()) {
      groupProvider = (GroupProvider) combinedProvider.get();
    } else {
      groupProvider = ConfluentBuiltInProviders.loadGroupProvider(groupProviderName);
      groupProvider.configure(originals());
    }
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
