// (Copyright) [2018 - 2018] Confluent, Inc.
package io.confluent.kafka.multitenant.integration.test;

import io.confluent.kafka.multitenant.PhysicalClusterMetadata;
import io.confluent.kafka.multitenant.integration.cluster.LogicalCluster;
import io.confluent.kafka.multitenant.integration.cluster.PhysicalCluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import kafka.security.auth.SimpleAclAuthorizer;
import kafka.security.auth.SimpleAclAuthorizer$;
import kafka.server.KafkaConfig$;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class TenantIsolationTest {

  private static final int BROKER_COUNT = 2;

  private IntegrationTestHarness testHarness;
  private LogicalCluster logicalCluster1;
  private LogicalCluster logicalCluster2;
  private PhysicalCluster physicalCluster;

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    testHarness = new IntegrationTestHarness(BROKER_COUNT);
    physicalCluster = testHarness.start(brokerProps());

    logicalCluster1 = physicalCluster.createLogicalCluster("tenantA", 100, 9, 11, 12);
    logicalCluster2 = physicalCluster.createLogicalCluster("tenantB", 200, 9, 21, 22);
  }

  @After
  public void tearDown() throws Exception {
    testHarness.shutdown();
  }

  private Properties brokerProps() throws IOException {
    Properties props = new Properties();
    props.put(KafkaConfig$.MODULE$.AuthorizerClassNameProp(), SimpleAclAuthorizer.class.getName());
    props.put(SimpleAclAuthorizer$.MODULE$.AllowEveryoneIfNoAclIsFoundProp(), "true");
    props.put(ConfluentConfigs.MULTITENANT_METADATA_CLASS_CONFIG,
              "io.confluent.kafka.multitenant.PhysicalClusterMetadata");
    props.put(ConfluentConfigs.MULTITENANT_METADATA_DIR_CONFIG,
                tempFolder.getRoot().getCanonicalPath());
    return props;
  }

  @Test
  public void testMultiTenantMetadataInstances() {
    List<String> brokerSessionUuids = physicalCluster.kafkaCluster().brokers().stream()
        .map(broker -> {
          Object cfgVal = broker.config().values().get(KafkaConfig$.MODULE$.BrokerSessionUuidProp());
          return cfgVal == null ? "" : cfgVal.toString();
        })
        .distinct()
        .collect(Collectors.toList());
    assertEquals("Expect each broker to have unique session UUID.",
                 BROKER_COUNT, brokerSessionUuids.size());
    brokerSessionUuids.forEach(uuid -> assertNotNull(
        "Expect valid instance of PhysicalClusterMetadata for broker session UUID " + uuid,
        PhysicalClusterMetadata.getInstance(uuid)));
  }

  @Test
  public void testProduceConsume() throws Throwable {
    // Users 1 and 2 belonging to the same cluster should be able to produce
    // and consume from the same `testtopic`
    testHarness.produceConsume(logicalCluster1.user(11), logicalCluster1.user(12), "testtopic", "group1", 0);

    // Users 3 and 4 belonging to a different cluster should be able to produce
    // and consume different set of messages from `testtopic`
    testHarness.produceConsume(logicalCluster2.user(21), logicalCluster2.user(22), "testtopic", "group1", 1000);

  }

  @Test
  public void testAcls() throws Throwable {

    AdminClient adminClient1 = testHarness.createAdminClient(logicalCluster1.adminUser());
    AdminClient adminClient2 = testHarness.createAdminClient(logicalCluster2.adminUser());

    assertEquals(Collections.emptySet(), describeAllAcls(adminClient1));

    List<ResourceType> resourceTypes = Arrays.asList(
        ResourceType.TOPIC,
        ResourceType.GROUP,
        ResourceType.TRANSACTIONAL_ID
    );
    Set<AclBinding> acls = new HashSet<>();
    resourceTypes.forEach(resourceType ->
      acls.add(new AclBinding(
          new ResourcePattern(resourceType, "test.resource", PatternType.LITERAL),
          new AccessControlEntry(logicalCluster1.user(11).unprefixedKafkaPrincipal().toString(), "*",
              AclOperation.WRITE, AclPermissionType.ALLOW))));
    resourceTypes.forEach(resourceType ->
      acls.add(new AclBinding(
          new ResourcePattern(resourceType, "test.", PatternType.PREFIXED),
          new AccessControlEntry(logicalCluster1.user(12).unprefixedKafkaPrincipal().toString(), "*",
              AclOperation.READ, AclPermissionType.ALLOW))));
    resourceTypes.forEach(resourceType ->
      acls.add(new AclBinding(
          new ResourcePattern(resourceType, "*", PatternType.LITERAL),
          new AccessControlEntry(logicalCluster1.user(11).unprefixedKafkaPrincipal().toString(), "*",
              AclOperation.DESCRIBE, AclPermissionType.ALLOW))));

    adminClient1.createAcls(acls).all().get();
    assertEquals(acls,  describeAllAcls(adminClient1));

    assertEquals(Collections.emptySet(), describeAllAcls(adminClient2));
    adminClient2.createAcls(acls).all().get();
    assertEquals(acls,  describeAllAcls(adminClient2));


    adminClient2.deleteAcls(Collections.singletonList(
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, "test", PatternType.PREFIXED),
            new AccessControlEntryFilter("User:*", "*", AclOperation.ANY, AclPermissionType.ANY)
    ))).all().get();
  }

  private Set<AclBinding> describeAllAcls(AdminClient adminClient) throws Exception {
    Collection<AclBinding> acls = adminClient.describeAcls(new AclBindingFilter(
        new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
        new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY)
    )).values().get();
    return new HashSet<>(acls);
  }
}
