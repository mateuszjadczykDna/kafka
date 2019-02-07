// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.common.license.LicenseValidator;
import io.confluent.kafka.security.authorizer.acl.AclMapper;
import io.confluent.kafka.security.authorizer.acl.AclProvider;
import io.confluent.license.validator.ConfluentLicenseValidator;
import java.util.Map;
import java.util.Optional;
import kafka.network.RequestChannel;
import kafka.network.RequestChannel.Session;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;


public class ConfluentKafkaAuthorizer extends AbstractConfluentAuthorizer implements Authorizer {

  private Authorizer aclAuthorizer;

  public ConfluentKafkaAuthorizer() {
    this(Time.SYSTEM);
  }

  public ConfluentKafkaAuthorizer(Time time) {
    super(time);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    Optional<Authorizer> aclProvider = accessRuleProviders().stream()
        .filter(a -> a instanceof AclProvider)
        .findFirst()
        .map(a -> (Authorizer) a);
    aclAuthorizer = aclProvider.orElse(new AclErrorProvider());
  }

  @Override
  public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {
    String host = session.clientAddress().getHostAddress();
    return super.authorize(session.principal(),
        host,
        AclMapper.operation(operation),
        AclMapper.resource(resource));
  }

  @Override
  public void addAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
    aclAuthorizer.addAcls(acls, resource);
  }

  @Override
  public boolean removeAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
    return aclAuthorizer.removeAcls(acls, resource);
  }

  @Override
  public boolean removeAcls(Resource resource) {
    return aclAuthorizer.removeAcls(resource);
  }

  @Override
  public scala.collection.immutable.Set<Acl> getAcls(Resource resource) {
    return aclAuthorizer.getAcls(resource);
  }

  @Override
  public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls(KafkaPrincipal principal) {
    return aclAuthorizer.getAcls(principal);
  }

  @Override
  public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls() {
    return aclAuthorizer.getAcls();
  }

  @Override
  public void close() {
    log.debug("Closing Kafka authorizer");
    super.close();
  }

  @Override
  protected LicenseValidator licenseValidator() {
    return new ConfluentLicenseValidator();
  }

  private static class AclErrorProvider implements Authorizer {

    private static final InvalidRequestException EXCEPTION =
        new InvalidRequestException("ACL-based authorization is disabled");

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public boolean authorize(Session session, Operation operation, Resource resource) {
      throw new IllegalStateException("Authprization not supported by this provider");
    }

    @Override
    public void addAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
      throw EXCEPTION;
    }

    @Override
    public boolean removeAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
      throw EXCEPTION;
    }

    @Override
    public boolean removeAcls(Resource resource) {
      throw EXCEPTION;
    }

    @Override
    public scala.collection.immutable.Set<Acl> getAcls(Resource resource) {
      throw EXCEPTION;
    }

    @Override
    public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls(KafkaPrincipal principal) {
      throw EXCEPTION;
    }

    @Override
    public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls() {
      throw EXCEPTION;
    }

    @Override
    public void close() {
    }
  }
}
