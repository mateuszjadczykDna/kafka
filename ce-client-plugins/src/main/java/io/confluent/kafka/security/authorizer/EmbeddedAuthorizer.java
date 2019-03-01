// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.common.license.LicenseExpiredException;
import io.confluent.kafka.common.license.InvalidLicenseException;
import io.confluent.kafka.common.license.LicenseValidator;
import io.confluent.kafka.security.authorizer.provider.AccessRuleProvider;
import io.confluent.kafka.security.authorizer.provider.MetadataProvider;
import io.confluent.kafka.security.authorizer.provider.Provider;
import io.confluent.kafka.security.authorizer.provider.ProviderFailedException;
import io.confluent.kafka.security.authorizer.provider.GroupProvider;
import io.confluent.kafka.security.authorizer.provider.InvalidScopeException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-component embedded authorizer that implements common authorization logic. This
 * authorizer loads configured providers and uses them to perform authorization.
 */
public class EmbeddedAuthorizer implements Authorizer {

  protected static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  private static final LicenseValidator DUMMY_LICENSE_VALIDATOR = new DummyLicenseValidator();

  private static final String ZK_CONNECT_PROPNAME = "zookeeper.connect";
  private static final String METRIC_GROUP = "confluent.license";
  private static final Map<Operation, Collection<Operation>> IMPLICIT_ALLOWED_OPS;

  private final Time time;
  private final Set<Provider> providersCreated;
  private LicenseValidator licenseValidator;
  private GroupProvider groupProvider;
  private List<AccessRuleProvider> accessRuleProviders;
  private MetadataProvider metadataProvider;
  private boolean allowEveryoneIfNoAcl;
  private String scope;

  static {
    IMPLICIT_ALLOWED_OPS = new HashMap<>();
    IMPLICIT_ALLOWED_OPS.put(new Operation("Describe"),
        Stream.of("Describe", "Read", "Write", "Delete", "Alter")
            .map(Operation::new).collect(Collectors.toSet()));
    IMPLICIT_ALLOWED_OPS.put(new Operation("DescribeConfigs"),
        Stream.of("DescribeConfigs", "AlterConfigs")
            .map(Operation::new).collect(Collectors.toSet()));
  }

  public EmbeddedAuthorizer() {
    this(Time.SYSTEM);
  }

  public EmbeddedAuthorizer(Time time) {
    this.time = time;
    this.providersCreated = new HashSet<>();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    ConfluentAuthorizerConfig authorizerConfig = new ConfluentAuthorizerConfig(configs);
    allowEveryoneIfNoAcl = authorizerConfig.allowEveryoneIfNoAcl;
    scope = authorizerConfig.scope;

    licenseValidator = licenseValidator();
    if (licenseValidator != null) {
      initializeAndValidateLicense(configs, licensePropName());
    }
    ConfluentAuthorizerConfig.Providers providers = authorizerConfig.createProviders();
    providersCreated.addAll(providers.accessRuleProviders);
    if (providers.groupProvider != null)
      providersCreated.add(providers.groupProvider);
    if (providers.metadataProvider != null)
      providersCreated.add(providers.metadataProvider);

    configureProviders(providers.accessRuleProviders,
        providers.groupProvider,
        providers.metadataProvider);
  }

  @Override
  public List<AuthorizeResult> authorize(KafkaPrincipal sessionPrincipal, String host, List<Action> actions) {
    return  actions.stream()
        .map(action -> authorize(sessionPrincipal, host, action))
        .collect(Collectors.toList());
  }

  public GroupProvider groupProvider() {
    return groupProvider;
  }

  public AccessRuleProvider accessRuleProvider(String providerName) {
    Optional<AccessRuleProvider> provider = accessRuleProviders.stream()
        .filter(p -> p.providerName().equals(providerName))
        .findFirst();
    if (provider.isPresent())
      return provider.get();
    else
      throw new IllegalArgumentException("Access rule provider not found: " + providerName);
  }

  public CompletableFuture<Void> start() {
    Set<Provider> providers = new HashSet<>(); // Use a set to remove duplicates
    if (groupProvider != null)
      providers.add(groupProvider);
    providers.addAll(accessRuleProviders);
    if (metadataProvider != null)
      providers.add(metadataProvider);
    List<CompletableFuture<Void>> futures = providers.stream()
        .map(Provider::start).map(CompletionStage::toCompletableFuture)
        .collect(Collectors.toList());
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  protected List<AccessRuleProvider> accessRuleProviders() {
    return accessRuleProviders;
  }

  protected void configureProviders(List<AccessRuleProvider> accessRuleProviders,
                                    GroupProvider groupProvider,
                                    MetadataProvider metadataProvider) {
    this.accessRuleProviders = accessRuleProviders;
    this.groupProvider = groupProvider;
    this.metadataProvider = metadataProvider;
  }

  private AuthorizeResult authorize(KafkaPrincipal sessionPrincipal, String host, Action action) {
    try {
      // On license expiry, update metric and log error, but continue to authorize
      if (licenseValidator != null)
        licenseValidator.verifyLicense(false);

      Set<KafkaPrincipal> groupPrincipals = groupProvider.groups(sessionPrincipal);
      boolean authorized = authorize(sessionPrincipal, groupPrincipals, host, action);
      logAuditMessage(sessionPrincipal, authorized, action.operation(), action.resource(), host);
      return authorized ? AuthorizeResult.ALLOWED : AuthorizeResult.DENIED;

    } catch (InvalidScopeException e) {
      log.error("Authorizer failed with unknown scope: {}", action.scope(), e);
      return AuthorizeResult.UNKNOWN_SCOPE;
    } catch (ProviderFailedException e) {
      log.error("Authorization provider has failed", e);
      return AuthorizeResult.AUTHORIZER_FAILED;
    } catch (Throwable t) {
      log.error("Authorization failed with unexpected exception", t);
      return AuthorizeResult.UNKNOWN_ERROR;
    }
  }

  private boolean authorize(KafkaPrincipal sessionPrincipal,
      Set<KafkaPrincipal> groupPrincipals,
      String host,
      Action action) {

    if (accessRuleProviders.stream()
        .anyMatch(p -> p.isSuperUser(sessionPrincipal, groupPrincipals, action.scope()))) {
      return true;
    }

    Resource resource = action.resource();
    Operation operation = action.operation();
    Set<AccessRule> rules = new HashSet<>();
    accessRuleProviders.stream()
        .filter(AccessRuleProvider::mayDeny)
        .forEach(p -> rules.addAll(p.accessRules(sessionPrincipal, groupPrincipals, action.scope(), action.resource())));

    // Check if there is any Deny acl match that would disallow this operation.
    if (aclMatch(operation, resource, host, PermissionType.DENY, rules))
      return false;

    accessRuleProviders.stream()
        .filter(p -> !p.mayDeny())
        .forEach(p -> rules.addAll(p.accessRules(sessionPrincipal, groupPrincipals, action.scope(), action.resource())));

    // Check if there are any Allow ACLs which would allow this operation.
    if (allowOps(operation).stream().anyMatch(op -> aclMatch(op, resource, host, PermissionType.ALLOW, rules)))
      return true;

    return isEmptyAclAndAuthorized(resource, rules);
  }

  @Override
  public void close() {
    AtomicReference<Throwable> firstException = new AtomicReference<>();
    providersCreated.forEach(provider ->
        ClientUtils.closeQuietly(provider, provider.providerName(), firstException));
    ClientUtils.closeQuietly(licenseValidator, "licenseValidator", firstException);
    Throwable exception = firstException.getAndSet(null);
    if (exception != null)
      throw new KafkaException("Failed to close authorizer cleanly", exception);
  }

  protected String scope() {
    return scope;
  }

  // Allow authorizer implementation to override so that LdapAuthorizer can provide its custom property
  protected String licensePropName() {
    return ConfluentAuthorizerConfig.LICENSE_PROP;
  }

  // Allow authorizer implementation to override so that LdapAuthorizer can provide its custom metric
  protected String licenseStatusMetricGroup() {
    return METRIC_GROUP;
  }

  // Allow Kafka brokers to override license validator. Other services (e.g. Metadata service)
  // will be performing license validation separately, so use a dummy validator as default.
  protected LicenseValidator licenseValidator() {
    return DUMMY_LICENSE_VALIDATOR;
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

  private static class DummyLicenseValidator implements LicenseValidator {

    @Override
    public void initializeAndVerify(String license, String zkConnect, Time time, String metricGroup) {
    }

    @Override
    public void verifyLicense(boolean failOnError) {
    }

    @Override
    public void close() {
    }
  }
}
