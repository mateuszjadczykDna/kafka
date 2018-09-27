// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.security.ldap.authorizer;

import io.confluent.kafka.security.ldap.license.InvalidLicenseException;
import io.confluent.kafka.security.ldap.license.LicenseExpiredException;
import io.confluent.kafka.security.ldap.license.LicenseValidator;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import kafka.network.RequestChannel;
import kafka.security.auth.Acl;
import kafka.security.auth.All$;
import kafka.security.auth.Allow$;
import kafka.security.auth.Alter$;
import kafka.security.auth.AlterConfigs$;
import kafka.security.auth.Delete$;
import kafka.security.auth.Deny$;
import kafka.security.auth.Describe$;
import kafka.security.auth.DescribeConfigs$;
import kafka.security.auth.Operation;
import kafka.security.auth.PermissionType;
import kafka.security.auth.Read$;
import kafka.security.auth.Resource;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.security.auth.Write$;

import kafka.server.KafkaConfig$;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;


public class LdapAuthorizer extends SimpleAclAuthorizer {

  private static final Logger authorizerLogger = LoggerFactory.getLogger("kafka.authorizer.logger");
  public static final String GROUP_PRINCIPAL_TYPE = "Group";
  private static final KafkaPrincipal WILDCARD_GROUP_PRINCIPAL =
      new KafkaPrincipal(GROUP_PRINCIPAL_TYPE, "*");

  private final Time time;
  private LicenseValidator licenseValidator;
  private LdapGroupManager groupManager;
  private boolean allowEveryoneIfNoAcl;
  private Set<KafkaPrincipal> superUsers;

  public LdapAuthorizer() {
    this(Time.SYSTEM);
  }

  public LdapAuthorizer(Time time) {
    this.time = time;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    LdapAuthorizerConfig authorizerConfig = new LdapAuthorizerConfig(configs);
    groupManager = new LdapGroupManager(authorizerConfig, time);
    groupManager.start();
    allowEveryoneIfNoAcl = authorizerConfig.allowEveryoneIfNoAcl;
    String su = authorizerConfig.superUsers;
    if (su != null && !su.trim().isEmpty()) {
      String[] users = su.split(";");
      superUsers = Arrays.stream(users)
          .map(user -> SecurityUtils.parseKafkaPrincipal(user.trim()))
          .collect(Collectors.toSet());
    } else {
      superUsers = Collections.emptySet();
    }
    String zkConnect = (String) configs.get(KafkaConfig$.MODULE$.ZkConnectProp());
    try {
      licenseValidator = new LicenseValidator(authorizerConfig.license, zkConnect, time);
    } catch (InvalidLicenseException | LicenseExpiredException e) {
      throw new InvalidLicenseException("LDAP Authorizer license validation failed."
          + " Please specify a valid license in the config " + LdapAuthorizerConfig.LICENSE_PROP
          + " to enable LDAP group-based authorization. Kafka brokers may be started with"
          + " user-principal based authorization using 'kafka.security.auth.SimpleAclAuthorizer'"
          + " without a license.", e);
    }

    super.configure(configs);
  }

  @Override
  public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {
    // On license expiry, update metric and log error, but continue to authorize
    licenseValidator.verifyLicense(false);

    // If LDAP group manager has failed with an exception and hasn't recovered within
    // the retry timeout, deny all access
    if (groupManager.failed()) {
      authorizerLogger.error("LDAP group manager has failed, denying all access");
      return false;
    }

    if (resource.patternType() != PatternType.LITERAL) {
      throw new IllegalArgumentException("Only literal resources are supported, got: "
          + resource.patternType());
    }

    // Always use KafkaPrincipal instance for comparisons since super.users and ACLs are
    // instantiated as KafkaPrincipal
    KafkaPrincipal sessionPrincipal = session.principal();
    KafkaPrincipal principal = sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;

    Set<String> groups = groupManager.groups(principal.getName());
    Set<KafkaPrincipal> groupPrincipals = groups.stream()
        .map(group -> new KafkaPrincipal(GROUP_PRINCIPAL_TYPE, group))
        .collect(Collectors.toSet());

    Set<Acl> acls = JavaConversions.setAsJavaSet(getMatchingAcls(resource.resourceType(),
        resource.name()));

    String host = session.clientAddress().getHostAddress();

    // Check if there is any Deny acl match that would disallow this operation.
    boolean denyMatch = userOrGroupMatch(Deny$.MODULE$, operation, resource, principal,
        groupPrincipals, host, acls);

    // Check if there are any Allow ACLs which would allow this operation.
    // Allowing read, write, delete, or alter implies allowing describe.
    // See #{org.apache.kafka.common.acl.AclOperation} for more details about ACL inheritance.
    Set<Operation> allowOps;
    if (operation == Describe$.MODULE$) {
      allowOps = Utils
          .mkSet(Describe$.MODULE$, Read$.MODULE$, Write$.MODULE$, Delete$.MODULE$, Alter$.MODULE$);
    } else if (operation == DescribeConfigs$.MODULE$) {
      allowOps = Utils.mkSet(DescribeConfigs$.MODULE$, AlterConfigs$.MODULE$);
    } else {
      allowOps = Collections.singleton(operation);
    }

    boolean allowMatch = allowOps.stream().anyMatch(op ->
        userOrGroupMatch(Allow$.MODULE$, op, resource, principal, groupPrincipals, host, acls));

    // we allow an operation if a user is a super user or if no acls are found and user has
    // configured to allow all users when no acls are found or if no deny acls are found and
    // at least one allow acls matches.
    boolean authorized = isSuperUserOrGroup(principal, groupPrincipals)
        || isEmptyAclAndAuthorized(resource, acls) || (!denyMatch && allowMatch);

    logAuditMessage(principal, authorized, operation, resource, host);
    return authorized;
  }

  @Override
  public void close() {
    super.close();
    if (groupManager != null) {
      groupManager.close();
    }
    if (licenseValidator != null) {
      licenseValidator.close();
    }
  }

  // Only for testing
  LdapGroupManager ldapGroupManager() {
    return groupManager;
  }

  private boolean userOrGroupMatch(PermissionType permissionType, Operation op, Resource resource,
      KafkaPrincipal principal, Set<KafkaPrincipal> groups, String host,
      Set<Acl> acls) {
    if (aclMatch(op, resource, principal, Acl.WildCardPrincipal(), host, permissionType, acls)) {
      return true;
    }
    for (KafkaPrincipal group : groups) {
      if (aclMatch(op, resource, group, WILDCARD_GROUP_PRINCIPAL, host, permissionType, acls)) {
        return true;
      }
    }
    return false;
  }

  // This is the same method as in SimpleAclAuthorizer, but it has been copied here
  // since the method in SimpleAclAuthorizer is private.
  private boolean aclMatch(Operation op, Resource resource, KafkaPrincipal principal,
      KafkaPrincipal wildcardPrincipal, String host, PermissionType permissionType, Set<Acl> acls) {
    for (Acl acl : acls) {
      if (acl.permissionType().equals(permissionType)
          && (acl.principal().equals(principal) || acl.principal().equals(wildcardPrincipal))
          && (op.equals(acl.operation()) || acl.operation().equals(All$.MODULE$))
          && (acl.host().equals(host) || acl.host().equals(Acl.WildCardHost()))) {
        authorizerLogger
            .debug("operation = {} on resource = {} from host = {} is {} based on acl = {}",
                op, resource, host, permissionType, acl);
        return true;
      }
    }
    return false;
  }


  private boolean isEmptyAclAndAuthorized(Resource resource, Set<Acl> acls) {
    if (acls.isEmpty()) {
      authorizerLogger
          .debug("No acl found for resource {}, authorized = {}", resource, allowEveryoneIfNoAcl);
      return allowEveryoneIfNoAcl;
    } else {
      return false;
    }
  }

  private boolean isSuperUserOrGroup(KafkaPrincipal userPrincipal, Set<KafkaPrincipal> groups) {
    if (superUsers.contains(userPrincipal)) {
      authorizerLogger
          .debug("principal = {} is a super user, allowing operation without checking acls.",
              userPrincipal);
      return true;
    } else {
      for (KafkaPrincipal group : groups) {
        if (superUsers.contains(group)) {
          authorizerLogger.debug(
              "principal = {} belongs to super group {}, allowing operation without checking acls.",
              userPrincipal, group);
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Log using the same format as SimpleAclAuthorizer:
   * <pre>
   *  def logMessage: String = {
   *    val authResult = if (authorized) "Allowed" else "Denied"
   *    s"Principal = $principal is $authResult Operation = $operation from host = $host on resource
   * = $resource"
   *  }
   * </pre>
   */
  private void logAuditMessage(KafkaPrincipal principal, boolean authorized, Operation op,
      Resource resource, String host) {
    String logMessage = "Principal = {} is {} Operation = {} from host = {} on resource = {}";
    if (authorized) {
      authorizerLogger.debug(logMessage, principal, "Allowed", op, host, resource);
    } else {
      authorizerLogger.info(logMessage, principal, "Denied", op, host, resource);
    }
  }
}
