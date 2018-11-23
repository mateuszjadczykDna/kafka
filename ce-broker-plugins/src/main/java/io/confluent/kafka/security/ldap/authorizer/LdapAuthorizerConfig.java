// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.ldap.authorizer;

import io.confluent.kafka.security.ldap.utils.ConfigurableSslSocketFactory;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Locale;
import java.util.stream.Collectors;
import kafka.security.auth.SimpleAclAuthorizer;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import javax.naming.Context;
import javax.naming.directory.SearchControls;
import java.net.URI;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;

public class LdapAuthorizerConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  public static final String CONFIG_PREFIX = "ldap.authorizer.";
  public static final String JAVA_NAMING_SOCKET_FACTORY_PROP = "java.naming.ldap.factory.socket";

  public static final String REFRESH_INTERVAL_MS_PROP = "ldap.authorizer.refresh.interval.ms";
  public static final int REFRESH_INTERVAL_MS_DEFAULT = 60 * 1000;
  public static final int PERSISTENT_REFRESH = 0;
  public static final String REFRESH_INTERVAL_MS_DOC =
      "LDAP group cache refresh interval in milliseconds. If set to zero, persistent LDAP search"
          + " is used.";

  public static final String SEARCH_PAGE_SIZE_PROP = "ldap.authorizer.search.page.size";
  public static final int SEARCH_PAGE_SIZE_DEFAULT = 0;
  public static final String SEARCH_PAGE_SIZE_DOC =
      "Page size for LDAP search if persistent search is disabled (refresh interval is greater"
          + " than zero). Paging is disabled by default.";

  public static final String RETRY_TIMEOUT_MS_PROP = "ldap.authorizer.retry.timeout.ms";
  public static final int RETRY_TIMEOUT_MS_DEFAULT = 60 * 60 * 1000;
  public static final String RETRY_TIMEOUT_MS_DOC =
      "Timeout for LDAP search retries after which the LDAP authorizer is marked as failed."
          + " All requests are denied access if a successful cache refresh cannot be performed"
          + " within this time.";

  public static final String RETRY_BACKOFF_MS_PROP = "ldap.authorizer.retry.backoff.ms";
  public static final int RETRY_BACKOFF_MS_DEFAULT = 100;
  public static final String RETRY_BACKOFF_MS_DOC =
      "Initial retry backoff in milliseconds. Exponential backoff is used if"
          + " 'ldap.authorizer.retry.backoff.max.ms' is set to a higher value.";

  public static final String RETRY_BACKOFF_MAX_MS_PROP = "ldap.authorizer.retry.backoff.max.ms";
  public static final int RETRY_BACKOFF_MAX_MS_DEFAULT = 1000;
  public static final String RETRY_BACKOFF_MAX_MS_DOC =
      "Maximum retry backoff in milliseconds. Exponential backoff is used if"
          + " 'ldap.authorizer.retry.backoff.ms' is set to a lower value.";

  public static final String SEARCH_MODE_PROP = "ldap.authorizer.search.mode";
  public static final String SEARCH_MODE_DEFAULT = SearchMode.GROUPS.name();
  public static final String SEARCH_MODE_DOC = "LDAP search mode that indicates if user to"
      + " group mapping is retrieved by searching for group or user entries. Valid values are USERS"
      + " and GROUPS.";

  public static final String GROUP_SEARCH_BASE_PROP = "ldap.authorizer.group.search.base";
  public static final String GROUP_SEARCH_BASE_DEFAULT = "ou=groups";
  public static final String GROUP_SEARCH_BASE_DOC = "LDAP search base for group-based search.";

  public static final String GROUP_SEARCH_FILTER_PROP = "ldap.authorizer.group.search.filter";
  public static final String GROUP_SEARCH_FILTER_DEFAULT = "";
  public static final String GROUP_SEARCH_FILTER_DOC = "LDAP search filter for group-based search.";

  public static final String GROUP_SEARCH_SCOPE_PROP = "ldap.authorizer.group.search.scope";
  public static final int GROUP_SEARCH_SCOPE_DEFAULT = SearchControls.ONELEVEL_SCOPE;
  public static final String GROUP_SEARCH_SCOPE_DOC =
      "LDAP search scope for group-based search. Valid values are 0 (OBJECT), 1 (ONELEVEL)"
          + " and 2 (SUBTREE).";

  public static final String GROUP_OBJECT_CLASS_PROP = "ldap.authorizer.group.object.class";
  public static final String GROUP_OBJECT_CLASS_DEFAULT = "groupOfNames";
  public static final String GROUP_OBJECT_CLASS_DOC = "LDAP object class for groups.";

  public static final String GROUP_NAME_ATTRIBUTE_PROP = "ldap.authorizer.group.name.attribute";
  public static final String GROUP_NAME_ATTRIBUTE_DEFAULT = "cn";
  public static final String GROUP_NAME_ATTRIBUTE_DOC =
      "Name of attribute that contains the name of the group in a group entry obtained using an"
          + " LDAP search. A regex pattern may be specified to extract the group name used in ACLs"
          + " from this attribute by configuring 'ldap.authorizer.group.name.attribute.pattern'.";

  public static final String GROUP_NAME_ATTRIBUTE_PATTERN_PROP =
      "ldap.authorizer.group.name.attribute.pattern";
  public static final String GROUP_NAME_ATTRIBUTE_PATTERN_DEFAULT = "";
  public static final String GROUP_NAME_ATTRIBUTE_PATTERN_DOC =
      "Java regular expression pattern used to extract the group name used in ACLs from the name of"
          + " the group obtained from the LDAP attribute specified using "
          + " 'ldap.authorizer.group.name.attribute`. By default the full value of the attribute is"
          + " used";

  public static final String GROUP_MEMBER_ATTRIBUTE_PROP = "ldap.authorizer.group.member.attribute";
  public static final String GROUP_MEMBER_ATTRIBUTE_DEFAULT = "member";
  public static final String GROUP_MEMBER_ATTRIBUTE_DOC =
      "Name of attribute that contains the members of the group in a group entry obtained using an"
          + " LDAP search. A regex pattern may be specified to extract the user principals"
          + " from this attribute by configuring 'ldap.authorizer.group.member.attribute.pattern'.";

  public static final String GROUP_MEMBER_ATTRIBUTE_PATTERN_PROP =
      "ldap.authorizer.group.member.attribute.pattern";
  public static final String GROUP_MEMBER_ATTRIBUTE_PATTERN_DEFAULT = "";
  public static final String GROUP_MEMBER_ATTRIBUTE_PATTERN_DOC =
      "Java regular expression pattern used to extract the user principals of group members from"
          + " group member entries obtained from the LDAP attribute specified using"
          + " 'ldap.authorizer.group.member.attribute`. By default the full value of the attribute"
          + " is used";

  public static final String GROUP_DN_NAME_PATTERN_PROP = "ldap.authorizer.group.dn.name.pattern";
  public static final String GROUP_DN_NAME_PATTERN_DEFAULT = "";
  public static final String GROUP_DN_NAME_PATTERN_DOC =
      "Java regular expression pattern used to extract group name from the distinguished name of"
          + " the group when group is renamed. This is used only when persistent search is enabled."
          + " By default the 'ldap.authorizer.group.name.attribute' is extracted from the DN";

  public static final String USER_SEARCH_BASE_PROP = "ldap.authorizer.user.search.base";
  public static final String USER_SEARCH_BASE_DEFAULT = "ou=users";
  public static final String USER_SEARCH_BASE_DOC = "LDAP search base for user-based search.";

  public static final String USER_SEARCH_FILTER_PROP = "ldap.authorizer.user.search.filter";
  public static final String USER_SEARCH_FILTER_DEFAULT = "";
  public static final String USER_SEARCH_FILTER_DOC = "LDAP search filter for user-based search.";

  public static final String USER_SEARCH_SCOPE_PROP = "ldap.authorizer.user.search.scope";
  public static final int USER_SEARCH_SCOPE_DEFAULT = SearchControls.ONELEVEL_SCOPE;
  public static final String USER_SEARCH_SCOPE_DOC =
      "LDAP search scope for user-based search. Valid values are 0 (OBJECT), 1 (ONELEVEL)"
          + " and 2 (SUBTREE).";

  public static final String USER_OBJECT_CLASS_PROP = "ldap.authorizer.user.object.class";
  public static final String USER_OBJECT_CLASS_DEFAULT = "person";
  public static final String USER_OBJECT_CLASS_DOC = "LDAP object class for users.";

  public static final String USER_NAME_ATTRIBUTE_PROP = "ldap.authorizer.user.name.attribute";
  public static final String USER_NAME_ATTRIBUTE_DEFAULT = "uid";
  public static final String USER_NAME_ATTRIBUTE_DOC =
      "Name of attribute that contains the user principal in a user entry obtained using an"
          + " LDAP search. A regex pattern may be specified to extract the user principal"
          + " from this attribute by configuring 'ldap.authorizer.user.name.attribute.pattern'.";

  public static final String USER_NAME_ATTRIBUTE_PATTERN_PROP =
      "ldap.authorizer.user.name.attribute.pattern";
  public static final String USER_NAME_ATTRIBUTE_PATTERN_DEFAULT = "";
  public static final String USER_NAME_ATTRIBUTE_PATTERN_DOC =
      "Java regular expression pattern used to extract the user principal from the name of"
          + " the user obtained from the LDAP attribute specified using"
          + " 'ldap.authorizer.user.name.attribute`. By default the full value of the attribute is"
          + " used";

  public static final String USER_MEMBEROF_ATTRIBUTE_PROP =
      "ldap.authorizer.user.memberof.attribute";
  public static final String USER_MEMBEROF_ATTRIBUTE_DEFAULT = "memberof";
  public static final String USER_MEMBEROF_ATTRIBUTE_DOC =
      "Name of attribute that contains the groups in a user entry obtained using an LDAP search."
          + " A regex pattern may be specified to extract the group names used in ACLs from this"
          + " attribute by configuring 'ldap.authorizer.user.memberof.attribute.pattern'.";

  public static final String USER_MEMBEROF_ATTRIBUTE_PATTERN_PROP =
      "ldap.authorizer.user.memberof.attribute.pattern";
  public static final String USER_MEMBEROF_ATTRIBUTE_PATTERN_DEFAULT = "";
  public static final String USER_MEMBEROF_ATTRIBUTE_PATTERN_DOC =
      "Java regular expression pattern used to extract the names of groups from user entries"
          + " obtained from the LDAP attribute specified using "
          + " 'ldap.authorizer.user.memberof.attribute`. By default the full value of the attribute"
          + " is used";

  public static final String USER_DN_NAME_PATTERN_PROP = "ldap.authorizer.user.dn.name.pattern";
  public static final String USER_DN_NAME_PATTERN_DEFAULT = "";
  public static final String USER_DN_NAME_PATTERN_DOC =
      "Java regular expression pattern used to extract user name from the distinguished name of"
          + " the user when user is renamed. This is used only when persistent search is enabled."
          + " By default the 'ldap.authorizer.user.name.attribute' is extracted from the DN";

  // SimpleAclAuthorizer configs

  public static final String ALLOW_IF_NO_ACLS_PROP =
      SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp();
  public static final boolean ALLOW_IF_NO_ACLS_DEFAULT = false;
  public static final String ALLOW_IF_NO_ACLS_DOC =
      "Boolean flag that indicates if everyone is allowed access to a resource if no ACL is found.";

  public static final String SUPER_USERS_PROP = SimpleAclAuthorizer.SuperUsersProp();
  public static final String SUPER_USERS_DEFAULT = "";
  public static final String SUPER_USERS_DOC = "Semicolon-separated list of principals of"
      + " super users who are allowed access to all resources.";

  public static final String LICENSE_PROP = "ldap.authorizer.license";
  public static final String LICENSE_DEFAULT = "";
  public static final String LICENSE_DOC = "License for LDAP plugins.";

  // JNDI configs com.sun.jndi.ldap.connect.timeout and com.sun.jndi.ldap.read.timeout
  private static final String JNDI_CONNECT_TIMEOUT_MS_PROP = "com.sun.jndi.ldap.connect.timeout";
  public static final String JNDI_READ_TIMEOUT_MS_PROP = "com.sun.jndi.ldap.read.timeout";
  public static final long JNDI_CONNECT_TIMEOUT_MS_DEFAULT = 30000;
  public static final long JNDI_READ_TIMEOUT_MS_DEFAULT = 30000;


  static {
    CONFIG = new ConfigDef()
        .define(ALLOW_IF_NO_ACLS_PROP, Type.BOOLEAN, ALLOW_IF_NO_ACLS_DEFAULT,
            Importance.MEDIUM, ALLOW_IF_NO_ACLS_DOC)
        .define(SUPER_USERS_PROP, Type.STRING, SUPER_USERS_DEFAULT,
            Importance.MEDIUM, SUPER_USERS_DOC)
        .define(REFRESH_INTERVAL_MS_PROP, Type.INT, REFRESH_INTERVAL_MS_DEFAULT,
            Importance.MEDIUM, REFRESH_INTERVAL_MS_DOC)
        .define(SEARCH_PAGE_SIZE_PROP, Type.INT, SEARCH_PAGE_SIZE_DEFAULT,
            Importance.MEDIUM, SEARCH_PAGE_SIZE_DOC)
        .define(RETRY_BACKOFF_MS_PROP, Type.INT, RETRY_BACKOFF_MS_DEFAULT,
            Importance.MEDIUM, RETRY_BACKOFF_MS_DOC)
        .define(RETRY_BACKOFF_MAX_MS_PROP, Type.INT, RETRY_BACKOFF_MAX_MS_DEFAULT,
            Importance.MEDIUM, RETRY_BACKOFF_MAX_MS_DOC)
        .define(RETRY_TIMEOUT_MS_PROP, Type.LONG, RETRY_TIMEOUT_MS_DEFAULT,
            Importance.MEDIUM, RETRY_TIMEOUT_MS_DOC)
        .define(SEARCH_MODE_PROP, Type.STRING, SEARCH_MODE_DEFAULT,
            ConfigDef.ValidString.in(SearchMode.GROUPS.name(), SearchMode.USERS.name()),
            Importance.MEDIUM, SEARCH_MODE_DOC)
        .define(GROUP_SEARCH_BASE_PROP, Type.STRING, GROUP_SEARCH_BASE_DEFAULT,
            Importance.HIGH, GROUP_SEARCH_BASE_DOC)
        .define(GROUP_SEARCH_FILTER_PROP, Type.STRING, GROUP_SEARCH_FILTER_DEFAULT,
            Importance.MEDIUM, GROUP_SEARCH_FILTER_DOC)
        .define(GROUP_SEARCH_SCOPE_PROP, Type.INT, GROUP_SEARCH_SCOPE_DEFAULT,
            Importance.MEDIUM, GROUP_SEARCH_SCOPE_DOC)
        .define(GROUP_OBJECT_CLASS_PROP, Type.STRING, GROUP_OBJECT_CLASS_DEFAULT,
            Importance.MEDIUM, GROUP_OBJECT_CLASS_DOC)
        .define(GROUP_NAME_ATTRIBUTE_PROP, Type.STRING, GROUP_NAME_ATTRIBUTE_DEFAULT,
            Importance.HIGH, GROUP_NAME_ATTRIBUTE_DOC)
        .define(GROUP_NAME_ATTRIBUTE_PATTERN_PROP, Type.STRING,
            GROUP_NAME_ATTRIBUTE_PATTERN_DEFAULT,
            Importance.LOW, GROUP_NAME_ATTRIBUTE_PATTERN_DOC)
        .define(GROUP_MEMBER_ATTRIBUTE_PROP, Type.STRING, GROUP_MEMBER_ATTRIBUTE_DEFAULT,
            Importance.HIGH, GROUP_MEMBER_ATTRIBUTE_DOC)
        .define(GROUP_MEMBER_ATTRIBUTE_PATTERN_PROP, Type.STRING,
            GROUP_MEMBER_ATTRIBUTE_PATTERN_DEFAULT,
            Importance.MEDIUM, GROUP_MEMBER_ATTRIBUTE_PATTERN_DOC)
        .define(GROUP_DN_NAME_PATTERN_PROP, Type.STRING,
            GROUP_DN_NAME_PATTERN_DEFAULT,
            Importance.LOW, GROUP_DN_NAME_PATTERN_DOC)
        .define(USER_SEARCH_BASE_PROP, Type.STRING, USER_SEARCH_BASE_DEFAULT,
            Importance.MEDIUM, USER_SEARCH_BASE_DOC)
        .define(USER_SEARCH_FILTER_PROP, Type.STRING, USER_SEARCH_FILTER_DEFAULT,
            Importance.MEDIUM, USER_SEARCH_FILTER_DOC)
        .define(USER_SEARCH_SCOPE_PROP, Type.INT, USER_SEARCH_SCOPE_DEFAULT,
            Importance.MEDIUM, USER_SEARCH_SCOPE_DOC)
        .define(USER_OBJECT_CLASS_PROP, Type.STRING, USER_OBJECT_CLASS_DEFAULT,
            Importance.MEDIUM, USER_OBJECT_CLASS_DOC)
        .define(USER_NAME_ATTRIBUTE_PROP, Type.STRING, USER_NAME_ATTRIBUTE_DEFAULT,
            Importance.MEDIUM, USER_NAME_ATTRIBUTE_DOC)
        .define(USER_NAME_ATTRIBUTE_PATTERN_PROP, Type.STRING, USER_NAME_ATTRIBUTE_PATTERN_DEFAULT,
            Importance.MEDIUM, USER_NAME_ATTRIBUTE_PATTERN_DOC)
        .define(USER_MEMBEROF_ATTRIBUTE_PROP, Type.STRING, USER_MEMBEROF_ATTRIBUTE_DEFAULT,
            Importance.MEDIUM, USER_MEMBEROF_ATTRIBUTE_DOC)
        .define(USER_MEMBEROF_ATTRIBUTE_PATTERN_PROP, Type.STRING,
            USER_MEMBEROF_ATTRIBUTE_PATTERN_DEFAULT,
            Importance.MEDIUM, USER_MEMBEROF_ATTRIBUTE_PATTERN_DOC)
        .define(USER_DN_NAME_PATTERN_PROP, Type.STRING,
            USER_DN_NAME_PATTERN_DEFAULT,
            Importance.LOW, USER_DN_NAME_PATTERN_DOC)
        .define(LICENSE_PROP, Type.STRING, LICENSE_DEFAULT,
            Importance.HIGH, LICENSE_DOC);

    // Add all SSL configs with Ldap prefix (we don't want to use base configs defined in the
    // broker, but we want to define these to configure SSL configs with consistent defaults).
    ConfigDef securityDefs = new ConfigDef();
    SslConfigs.addClientSslSupport(securityDefs);
    SaslConfigs.addClientSaslSupport(securityDefs);
    securityDefs.configKeys().values().forEach(configKey ->
        CONFIG.define(CONFIG_PREFIX + configKey.name,
            configKey.type,
            configKey.defaultValue,
            configKey.validator,
            configKey.importance,
            configKey.documentation));
  }

  public enum SearchMode {
    GROUPS,
    USERS
  }

  final boolean allowEveryoneIfNoAcl;
  final String superUsers;
  final boolean persistentSearch;
  final int refreshIntervalMs;
  final int searchPageSize;
  final long retryTimeoutMs;
  final int retryBackoffMs;
  final int retryMaxBackoffMs;
  final SearchMode searchMode;

  final String groupSearchBase;
  final String groupSearchFilter;
  final int groupSearchScope;
  final String groupNameAttribute;
  final Pattern groupNameAttributePattern;
  final String groupMemberAttribute;
  final Pattern groupMemberAttributePattern;
  final Pattern groupDnNamePattern;

  final String userSearchBase;
  final String userSearchFilter;
  final int userSearchScope;
  final String userNameAttribute;
  final Pattern userNameAttributePattern;
  final String userMemberOfAttribute;
  final Pattern userMemberOfAttributePattern;
  final Pattern userDnNamePattern;
  final String license;

  final Hashtable<String, String> ldapContextEnvironment;

  public LdapAuthorizerConfig(Map<?, ?> props) {
    super(CONFIG, props);

    allowEveryoneIfNoAcl = getBoolean(ALLOW_IF_NO_ACLS_PROP);
    superUsers = getString(SUPER_USERS_PROP);
    refreshIntervalMs = getInt(REFRESH_INTERVAL_MS_PROP);
    retryTimeoutMs = getLong(RETRY_TIMEOUT_MS_PROP);
    retryBackoffMs = getInt(RETRY_BACKOFF_MS_PROP);
    retryMaxBackoffMs = getInt(RETRY_BACKOFF_MAX_MS_PROP);

    persistentSearch = refreshIntervalMs == PERSISTENT_REFRESH;
    searchMode = SearchMode.valueOf(getString(SEARCH_MODE_PROP).toUpperCase(Locale.ROOT));
    searchPageSize = getInt(SEARCH_PAGE_SIZE_PROP);

    groupSearchBase = getString(GROUP_SEARCH_BASE_PROP);
    groupSearchFilter = searchFilter(GROUP_OBJECT_CLASS_PROP, GROUP_SEARCH_FILTER_PROP);
    groupSearchScope = getInt(GROUP_SEARCH_SCOPE_PROP);
    groupNameAttribute = getString(GROUP_NAME_ATTRIBUTE_PROP);
    groupNameAttributePattern = attributePattern(GROUP_NAME_ATTRIBUTE_PATTERN_PROP);
    groupMemberAttribute = getString(GROUP_MEMBER_ATTRIBUTE_PROP);
    groupMemberAttributePattern = attributePattern(GROUP_MEMBER_ATTRIBUTE_PATTERN_PROP);
    String pattern = getString(GROUP_DN_NAME_PATTERN_PROP);
    groupDnNamePattern = pattern.isEmpty() ? null : Pattern.compile(pattern);

    userSearchBase = getString(USER_SEARCH_BASE_PROP);
    userSearchFilter = searchFilter(USER_OBJECT_CLASS_PROP, USER_SEARCH_FILTER_PROP);
    userSearchScope = getInt(USER_SEARCH_SCOPE_PROP);
    userNameAttribute = getString(USER_NAME_ATTRIBUTE_PROP);
    userNameAttributePattern = attributePattern(USER_NAME_ATTRIBUTE_PATTERN_PROP);
    userMemberOfAttribute = getString(USER_MEMBEROF_ATTRIBUTE_PROP);
    userMemberOfAttributePattern = attributePattern(USER_MEMBEROF_ATTRIBUTE_PATTERN_PROP);
    pattern = getString(USER_DN_NAME_PATTERN_PROP);
    userDnNamePattern = pattern.isEmpty() ? null : Pattern.compile(pattern);

    license = getString(LICENSE_PROP);

    validate();

    ldapContextEnvironment = createLdapContextEnvironment();
  }

  private boolean sslEnabled() {
    String providerUrl = (String) originals().get(CONFIG_PREFIX + Context.PROVIDER_URL);
    String protocol = (String) originals().get(CONFIG_PREFIX + Context.SECURITY_PROTOCOL);
    return "ldaps".equals(URI.create(providerUrl).getScheme()) || "ssl".equalsIgnoreCase(protocol);
  }

  private Hashtable<String, String> createLdapContextEnvironment() {
    Hashtable<String, String> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");

    // Include all java.naming.* and com.sun.jndi.* configs for context creation
    unprefix(originals()).entrySet().stream()
        .filter(e -> e.getKey().startsWith("java.naming.")
            || e.getKey().startsWith("com.sun.jndi."))
        .forEach(e -> env.put(e.getKey(), String.valueOf(e.getValue())));

    // Configure socket factory for SSL if the factory is not explicitly overridden
    // We reuse the SslFactory from `kafka-clients`, so use the parsed configs from
    // `config.values()`.
    Map<String, Object> unprefixedConfigs = unprefix(values());
    if (sslEnabled()
        && !unprefixedConfigs.containsKey(LdapAuthorizerConfig.JAVA_NAMING_SOCKET_FACTORY_PROP)) {
      ConfigurableSslSocketFactory.createSslFactory(unprefixedConfigs);
      env.put(LdapAuthorizerConfig.JAVA_NAMING_SOCKET_FACTORY_PROP,
          ConfigurableSslSocketFactory.class.getName());
    }

    if (!env.containsKey(JNDI_CONNECT_TIMEOUT_MS_PROP)) {
      env.put(JNDI_CONNECT_TIMEOUT_MS_PROP,
          String.valueOf(LdapAuthorizerConfig.JNDI_CONNECT_TIMEOUT_MS_DEFAULT));
    }
    if (!env.containsKey(JNDI_READ_TIMEOUT_MS_PROP)) {
      env.put(JNDI_READ_TIMEOUT_MS_PROP,
          String.valueOf(LdapAuthorizerConfig.JNDI_READ_TIMEOUT_MS_DEFAULT));
    }
    return env;
  }

  private Map<String, Object> unprefix(Map<String, ?> map) {
    return map.entrySet().stream()
        .filter(e -> e.getKey().startsWith(LdapAuthorizerConfig.CONFIG_PREFIX)
            && e.getValue() != null)
        .collect(Collectors.toMap(
            e -> e.getKey().substring(LdapAuthorizerConfig.CONFIG_PREFIX.length()),
            e -> e.getValue()));
  }

  private String searchFilter(String objectClassProp, String filterProp) {
    String classFilter = "(objectClass=" + getString(objectClassProp) + ")";
    String configuredFilter = getString(filterProp);
    if (configuredFilter.isEmpty()) {
      return classFilter;
    } else {
      return String.format("(&%s%s)", classFilter, configuredFilter);
    }
  }

  private Pattern attributePattern(String patternProp) {
    String patternStr = getString(patternProp);
    if (patternStr == null || patternStr.isEmpty()) {
      return null;
    } else {
      return Pattern.compile(patternStr);
    }
  }

  private void validate() {
    if (!originals().containsKey(CONFIG_PREFIX + Context.PROVIDER_URL)) {
      throw new ConfigException("LDAP provider URL must be specified using the config "
          + CONFIG_PREFIX + Context.PROVIDER_URL);
    }
    if (retryTimeoutMs < refreshIntervalMs * 2) {
      throw new ConfigException(String.format("Retry timeout %s=%d should be at least twice %s=%d",
          RETRY_TIMEOUT_MS_PROP, retryTimeoutMs,
          REFRESH_INTERVAL_MS_PROP, refreshIntervalMs));
    }
    if (retryTimeoutMs < retryMaxBackoffMs) {
      throw new ConfigException(String.format("Retry timeout %s=%d should be at least %s=%d",
          RETRY_BACKOFF_MAX_MS_PROP, retryMaxBackoffMs,
          RETRY_TIMEOUT_MS_PROP, retryTimeoutMs));
    }
    if (retryMaxBackoffMs < retryBackoffMs) {
      throw new ConfigException(String.format("Retry max backoff %s=%d should be at least %s=%d",
          RETRY_BACKOFF_MAX_MS_PROP, retryMaxBackoffMs,
          RETRY_BACKOFF_MS_PROP, retryBackoffMs));
    }
  }

  @Override
  public String toString() {
    Map<String, String> env = new HashMap<>(ldapContextEnvironment);
    if (env.containsKey(Context.SECURITY_CREDENTIALS)) {
      env.put(Context.SECURITY_CREDENTIALS, Password.HIDDEN);
    }
    return String.format("LdapAuthorizerConfig: %n\t%s%n\t%s",
        Utils.mkString(values(), "", "", "=", "%n\t"),
        Utils.mkString(env, "", "", "=", "%n\t"));
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
