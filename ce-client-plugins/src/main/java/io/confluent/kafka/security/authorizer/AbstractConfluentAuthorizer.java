// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.common.license.LicenseExpiredException;
import io.confluent.kafka.common.license.InvalidLicenseException;
import io.confluent.kafka.common.license.LicenseValidator;
import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.security.authorizer.provider.GroupProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-component base authorizer that implements common authorization logic.
 */
public abstract class AbstractConfluentAuthorizer implements Configurable, AutoCloseable {

  protected static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  private static final String ZK_CONNECT_PROPNAME = "zookeeper.connect";
  private static final String METRIC_GROUP = "confluent.license";
  private static final Map<Operation, Collection<Operation>> IMPLICIT_ALLOWED_OPS;

  private final Time time;
  private LicenseValidator licenseValidator;
  private GroupProvider groupProvider;
  private List<AccessRuleProvider> accessRuleProviders;
  private boolean allowEveryoneIfNoAcl;

  static {
    IMPLICIT_ALLOWED_OPS = new HashMap<>();
    IMPLICIT_ALLOWED_OPS.put(new Operation("Describe"),
        Stream.of("Describe", "Read", "Write", "Delete", "Alter")
            .map(Operation::new).collect(Collectors.toSet()));
    IMPLICIT_ALLOWED_OPS.put(new Operation("DescribeConfigs"),
        Stream.of("DescribeConfigs", "AlterConfigs")
            .map(Operation::new).collect(Collectors.toSet()));
  }

  public AbstractConfluentAuthorizer() {
    this(Time.SYSTEM);
  }

  AbstractConfluentAuthorizer(Time time) {
    this.time = time;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    ConfluentAuthorizerConfig authorizerConfig = new ConfluentAuthorizerConfig(configs);
    groupProvider = authorizerConfig.groupProvider;
    accessRuleProviders = authorizerConfig.accessRuleProviders;
    allowEveryoneIfNoAcl = authorizerConfig.allowEveryoneIfNoAcl;

    licenseValidator = licenseValidator();
    if (licenseValidator != null) {
      initializeAndValidateLicense(configs, licensePropName());
    }
  }

  public boolean authorize(KafkaPrincipal sessionPrincipal, String host,
      Operation operation, Resource resource) {
    // On license expiry, update metric and log error, but continue to authorize
    if (licenseValidator != null)
      licenseValidator.verifyLicense(false);

    if (resource.patternType() != PatternType.LITERAL) {
      throw new IllegalArgumentException("Only literal resources are supported, got: "
          + resource.patternType());
    }

    Set<KafkaPrincipal> groupPrincipals = groupProvider.groups(sessionPrincipal);
    boolean authorized = authorize(sessionPrincipal, groupPrincipals, host, operation, resource);

    logAuditMessage(sessionPrincipal, authorized, operation, resource, host);
    return authorized;
  }

  public GroupProvider groupProvider() {
    return groupProvider;
  }

  protected List<AccessRuleProvider> accessRuleProviders() {
    return accessRuleProviders;
  }

  private boolean authorize(KafkaPrincipal sessionPrincipal,
      Set<KafkaPrincipal> groupPrincipals,
      String host,
      Operation operation,
      Resource resource) {

    if (accessRuleProviders.stream()
        .anyMatch(p -> p.isSuperUser(sessionPrincipal, groupPrincipals))) {
      return true;
    }

    Set<AccessRule> permissions = new HashSet<>();
    accessRuleProviders.stream()
        .filter(AccessRuleProvider::mayDeny)
        .forEach(p -> permissions.addAll(p.accessRules(sessionPrincipal, groupPrincipals, resource)));

    // Check if there is any Deny acl match that would disallow this operation.
    if (aclMatch(operation, resource, host, PermissionType.DENY, permissions))
      return false;

    accessRuleProviders.stream()
        .filter(p -> !p.mayDeny())
        .forEach(p -> permissions.addAll(p.accessRules(sessionPrincipal, groupPrincipals, resource)));

    // Check if there are any Allow ACLs which would allow this operation.
    if (allowOps(operation).stream().anyMatch(op -> aclMatch(op, resource, host, PermissionType.ALLOW, permissions)))
      return true;

    return isEmptyAclAndAuthorized(resource, permissions);
  }

  @Override
  public void close() {
    if (groupProvider != null)
      groupProvider.close();
    accessRuleProviders.forEach(AccessRuleProvider::close);
    if (licenseValidator != null) {
      licenseValidator.close();
    }
  }

  // Allow authorizer implementation to override so that LdapAuthorizer can provide its custom property
  protected String licensePropName() {
    return ConfluentAuthorizerConfig.LICENSE_PROP;
  }

  // Allow authorizer implementation to override so that LdapAuthorizer can provide its custom metric
  protected String licenseStatusMetricGroup() {
    return METRIC_GROUP;
  }

  private boolean aclMatch(Operation op,
      Resource resource,
      String host, PermissionType permissionType,
      Collection<AccessRule> permissions) {
    for (AccessRule acl : permissions) {
      if (acl.permissionType().equals(permissionType)
          && (op.equals(acl.operation()) || acl.operation().equals(Operation.ALL))
          && (acl.host().equals(host) || acl.host().equals(AccessRule.ALL_HOSTS))) {
        log.debug("operation = {} on resource = {} from host = {} is {} based on acl = {}",
            op, resource, host, permissionType, acl.sourceDescription());
        return true;
      }
    }
    return false;
  }

  private boolean isEmptyAclAndAuthorized(Resource resource, Set<AccessRule> acls) {
    if (acls.isEmpty()) {
      log.debug("No acl found for resource {}, authorized = {}", resource, allowEveryoneIfNoAcl);
      return allowEveryoneIfNoAcl;
    } else {
      return false;
    }
  }

  /**
   * Log using the same format as SimpleAclAuthorizer:
   * <pre>
   *  def logMessage: String = {
   *    val authResult = if (authorized) "Allowed" else "Denied"
   *    s"Principal = $principal is $authResult Operation = $operation from host = $host on
   * resource
   * = $resource"
   *  }
   * </pre>
   */
  private void logAuditMessage(KafkaPrincipal principal, boolean authorized,
      Operation op,
      Resource resource, String host) {
    String logMessage = "Principal = {} is {} Operation = {} from host = {} on resource = {}";
    if (authorized) {
      log.debug(logMessage, principal, "Allowed", op, host, resource);
    } else {
      log.info(logMessage, principal, "Denied", op, host, resource);
    }
  }

  // Allowing read, write, delete, or alter implies allowing describe.
  // See org.apache.kafka.common.acl.AclOperation for more details about ACL inheritance.
  private static Collection<Operation> allowOps(Operation operation) {
    Collection<Operation> allowOps = IMPLICIT_ALLOWED_OPS.get(operation);
    if (allowOps != null)
      return allowOps;
    else
      return Collections.singleton(operation);
  }

  abstract protected LicenseValidator licenseValidator();

  private void initializeAndValidateLicense(Map<String, ?> configs, String licensePropName) {
    String license = (String) configs.get(licensePropName);
    String zkConnect = (String) configs.get(ZK_CONNECT_PROPNAME);
    try {
      licenseValidator.initializeAndVerify(license, zkConnect, time, licenseStatusMetricGroup());
    } catch (InvalidLicenseException | LicenseExpiredException e) {
      throw new InvalidLicenseException(
          String.format("Confluent Authorizer license validation failed."
              + " Please specify a valid license in the config " + licensePropName
              + " to enable authorization using %s. Kafka brokers may be started with basic"
              + " user-principal based authorization using 'kafka.security.auth.SimpleAclAuthorizer'"
              + " without a license.", this.getClass().getName()), e);
    }
  }
}
