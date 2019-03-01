// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.provider.InvalidScopeException;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.RoleAssignmentKey;
import io.confluent.security.auth.store.data.RoleAssignmentValue;
import io.confluent.security.rbac.InvalidRoleAssignmentException;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.Scope;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.store.NotMasterWriterException;
import io.confluent.security.test.utils.RbacTestUtils;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaAuthWriterTest {

  private final Time time = new MockTime();
  private final KafkaPrincipal alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice");
  private final KafkaPrincipal bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob");
  private final int storeNodeId = 1;

  private MockAuthStore authStore;
  private KafkaAuthWriter authWriter;
  private DefaultAuthCache authCache;

  @Before
  public void setUp() throws Exception {
    RbacRoles rbacRoles = RbacRoles.load(this.getClass().getClassLoader(), "test_rbac_roles.json");
    authStore = MockAuthStore.create(rbacRoles, time, new Scope("testOrg"), 2, storeNodeId);
    authStore.startService(authStore.urls());
    assertNotNull(authStore.writer());
    authWriter = authStore.writer();
    authCache = authStore.authCache();
    TestUtils.waitForCondition(() -> authStore.masterWriterUrl("http") != null, "Writer not elected");
  }

  @After
  public void tearDown() {
    if (authStore != null)
      authStore.close();
    KafkaTestUtils.verifyThreadCleanup();
  }

  @Test
  public void testWriterElection() throws Exception {
    assertEquals(new URL("http://server1:8089"), authStore.masterWriterUrl("http"));
    assertEquals(new URL("https://server1:8090"), authStore.masterWriterUrl("https"));

    int newWriter = storeNodeId + 1;
    authStore.makeMasterWriter(newWriter);
    TestUtils.waitForCondition(() -> !authStore.url("http").equals(authStore.masterWriterUrl("http")),
        "Rebalance not completed");
    assertEquals(new URL("http://server2:8089"), authStore.masterWriterUrl("http"));
    assertEquals(new URL("https://server2:8090"), authStore.masterWriterUrl("https"));

    assertEquals(authStore.nodes.values().stream().map(n -> n.url("http")).collect(Collectors.toSet()),
        authStore.activeNodeUrls("http"));
    assertEquals(authStore.nodes.values().stream().map(n -> n.url("https")).collect(Collectors.toSet()),
        authStore.activeNodeUrls("https"));
  }

  @Test
  public void testClusterScopeAssignment() throws Exception {
    String clusterA = "testOrg/clusterA";
    String clusterB = "testOrg/clusterB";

    authWriter.addRoleAssignment(alice, "Cluster Admin", clusterA).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(alice, "Cluster Admin", clusterA));

    authWriter.addRoleAssignment(bob, "Operator", clusterB).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(bob, "Operator", clusterB));
    assertNull(rbacResources(bob, "Operator", clusterA));
    assertNull(rbacResources(bob, "Cluster Admin", clusterB));

    authWriter.addRoleAssignment(alice, "Operator", clusterA).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(alice, "Operator", clusterA));
    assertEquals(Collections.emptySet(), rbacResources(alice, "Cluster Admin", clusterA));

    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Cluster Admin", clusterA);
    assertNull(rbacResources(alice, "Cluster Admin", clusterA));
    assertEquals(Collections.emptySet(), rbacResources(alice, "Operator", clusterA));
    RbacTestUtils.deleteRoleAssignment(authCache, alice, "Operator", clusterA);
    assertNull(rbacResources(alice, "Operator", clusterA));
    assertEquals(Collections.emptySet(), rbacResources(bob, "Operator", clusterB));
  }

  @Test
  public void testResourceScopeAssignment() throws Exception {
    String clusterA = "testOrg/clusterA";
    String clusterB = "testOrg/clusterB";

    // Assign role without resources, add resources
    authWriter.addRoleAssignment(alice, "Reader", clusterA).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(alice, "Reader", clusterA));
    Collection<Resource> aliceResources = resources("aliceTopicA", "aliceGroupB");
    authWriter.addRoleResources(alice, "Reader", clusterA, aliceResources).toCompletableFuture().join();
    assertEquals(aliceResources, rbacResources(alice, "Reader", clusterA));
    Collection<Resource> resources2 = resources("aliceTopicA", "aliceGroupD");
    authWriter.addRoleResources(alice, "Reader", clusterA, resources2).toCompletableFuture().join();
    assertEquals(3, rbacResources(alice, "Reader", clusterA).size());
    aliceResources.addAll(resources2);
    assertEquals(aliceResources, rbacResources(alice, "Reader", clusterA));

    // Add resources without assigning first, this should assign role with resources
    Collection<Resource> bobResources = resources("bobTopic", "bobGroup");
    authWriter.addRoleResources(bob, "Writer", clusterB, bobResources).toCompletableFuture().join();
    assertEquals(bobResources, rbacResources(bob, "Writer", clusterB));
    assertNull(rbacResources(bob, "Writer", clusterA));

    // Set resources with group principal
    KafkaPrincipal finance = new KafkaPrincipal("Group", "finance");
    Collection<Resource> financeResources = resources("financeTopic", "financeGroup");
    authWriter.setRoleResources(finance, "Writer", clusterB, financeResources).toCompletableFuture().join();
    assertEquals(financeResources, rbacResources(finance, "Writer", clusterB));
    financeResources = resources("financeTopic2", "financeGroup");
    authWriter.setRoleResources(finance, "Writer", clusterB, financeResources).toCompletableFuture().join();
    assertEquals(financeResources, rbacResources(finance, "Writer", clusterB));
    authWriter.setRoleResources(finance, "Writer", clusterB, Collections.emptySet()).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(finance, "Writer", clusterB));
    authWriter.setRoleResources(finance, "Writer", clusterB, financeResources).toCompletableFuture().join();
    assertEquals(financeResources, rbacResources(finance, "Writer", clusterB));

    // Remove role
    authWriter.removeRoleAssignment(bob, "Writer", clusterA).toCompletableFuture().join();
    assertEquals(bobResources, rbacResources(bob, "Writer", clusterB));
    authWriter.removeRoleAssignment(bob, "Writer", clusterB).toCompletableFuture().join();
    assertNull(rbacResources(bob, "Writer", clusterB));

    // Remove role resources
    authWriter.removeRoleResources(alice, "Reader", clusterA, resources("some.topic", "some.group")).toCompletableFuture().join();
    assertEquals(aliceResources, rbacResources(alice, "Reader", clusterA));
    authWriter.removeRoleResources(alice, "Reader", clusterA, Collections.singleton(groupResource("aliceGroupB"))).toCompletableFuture().join();
    aliceResources.remove(groupResource("aliceGroupB"));
    assertEquals(aliceResources, rbacResources(alice, "Reader", clusterA));
    authWriter.removeRoleResources(alice, "Reader", clusterA, aliceResources).toCompletableFuture().join();
    assertEquals(Collections.emptySet(), rbacResources(alice, "Reader", clusterA));
    authWriter.removeRoleAssignment(alice, "Reader", clusterA).toCompletableFuture().join();
    assertNull(rbacResources(alice, "Reader", clusterA));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testClusterScopeAddResources() throws Exception {
    authWriter.addRoleResources(bob, "Operator", "testOrg/clusterA", resources("topicA", "groupB"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testClusterScopeRemoveResources() throws Exception {
    authWriter.removeRoleResources(bob, "Operator", "testOrg/clusterA", resources("topicA", "groupB"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testClusterScopeSetResources() throws Exception {
    authWriter.setRoleResources(bob, "Operator", "testOrg/clusterA", resources("topicA", "groupB"));
  }

  @Test(expected = InvalidRoleAssignmentException.class)
  public void testUnknownRoleAddAssignment() throws Exception {
    authWriter.addRoleAssignment(bob, "SomeRole", "testOrg/clusterA");
  }

  @Test(expected = InvalidRoleAssignmentException.class)
  public void testUnknownRoleAddResources() throws Exception {
    authWriter.addRoleResources(bob, "SomeRole", "testOrg/clusterA", resources("topicA", "groupB"));
  }

  @Test(expected = InvalidRoleAssignmentException.class)
  public void testUnknownRoleSetResources() throws Exception {
    authWriter.setRoleResources(bob, "SomeRole", "testOrg/clusterA", resources("topicA", "groupB"));
  }

  @Test(expected = InvalidRoleAssignmentException.class)
  public void testUnknownRoleRemoveResources() throws Exception {
    authWriter.removeRoleResources(bob, "SomeRole", "testOrg/clusterA", resources("topicA", "groupB"));
  }

  @Test(expected = InvalidRoleAssignmentException.class)
  public void testUnknownRoleRemoveAssignment() throws Exception {
    authWriter.removeRoleAssignment(bob, "SomeRole", "testOrg/clusterA");
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeAddAssignment() throws Exception {
    authWriter.addRoleAssignment(alice, "Operator", "anotherOrg/clusterA");
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeAddResources() throws Exception {
    authWriter.addRoleResources(alice, "Reader", "anotherOrg/clusterA", resources("topicA", "groupB"));
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeSetResources() throws Exception {
    authWriter.setRoleResources(alice, "Reader", "anotherOrg/clusterA", resources("topicA", "groupB"));
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeRemoveResources() throws Exception {
    authWriter.removeRoleResources(alice, "Reader", "anotherOrg/clusterA", resources("topicA", "groupB"));
  }

  @Test(expected = InvalidScopeException.class)
  public void testUnknownScopeRemoveAssignment() throws Exception {
    authWriter.removeRoleAssignment(alice, "Operator", "anotherOrg/clusterA");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidScopeAddAssignment() throws Exception {
    authWriter.addRoleAssignment(alice, "Operator", "//");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidScopeAddResources() throws Exception {
    authWriter.addRoleResources(alice, "Reader", "//", resources("topicA", "groupB"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidScopeSetResources() throws Exception {
    authWriter.setRoleResources(alice, "Reader", "//", resources("topicA", "groupB"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidScopeRemoveResources() throws Exception {
    authWriter.removeRoleResources(alice, "Reader", "//", resources("topicA", "groupB"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidScopeRemoveAssignment() throws Exception {
    authWriter.removeRoleAssignment(alice, "Operator", "//");
  }

  @Test(expected = NotMasterWriterException.class)
  public void testNoMasterWriter() throws Exception {
    authStore.makeMasterWriter(-1);
    TestUtils.waitForCondition(() -> !authStore.url("http").equals(authStore.masterWriterUrl("http")),
        "Not rebalancing");
    authWriter.addRoleAssignment(bob, "Operator", "testOrg/clusterA");
  }

  @Test(expected = NotMasterWriterException.class)
  public void testNewMasterWriter() throws Exception {
    authStore.makeMasterWriter(storeNodeId + 1);
    TestUtils.waitForCondition(() -> !authStore.url("http").equals(authStore.masterWriterUrl("http")),
        "Rebalance not complete");
    authWriter.addRoleAssignment(bob, "Operator", "testOrg/clusterA");
  }

  @Test
  public void testWriterReelectionBeforeProduceComplete() throws Exception {
    TestUtils.waitForCondition(() -> authCache.status(0) == MetadataStoreStatus.INITIALIZED,
        "Auth store not initialized");
    authStore.configureDelays(Long.MAX_VALUE, Long.MAX_VALUE); // Don't complete produce/consume

    CompletionStage<Void> stage1 = authWriter.addRoleAssignment(bob, "Reader", "testOrg/clusterA");
    CompletionStage<Void> stage2 = authWriter.setRoleResources(bob, "Reader", "testOrg/clusterA",
        resources("topicA", "groupA"));
    authWriter.stopWriter(1);
    authWriter.startWriter(2);
    authStore.producer.completeNext();

    // Write shouldn't complete even though local generation changed
    assertFalse(stage1.toCompletableFuture().isDone());
    assertFalse(stage2.toCompletableFuture().isDone());

    // Write should complete successfully if it is consumed before the new generation status record
    List<ProducerRecord<AuthKey, AuthValue>> sent = authStore.producer.history();
    authStore.consumer.addRecord(authStore.consumerRecord(sent.get(sent.size() - 2)));
    stage1.toCompletableFuture().get(10, TimeUnit.SECONDS);

    // Pending write should fail when new generation status record appears
    authStore.addNewGenerationStatusRecord(2);
    verifyFailure(stage2, NotMasterWriterException.class);
  }

  @Test
  public void testWriterReelectionBeforeConsumeComplete() throws Exception {
    TestUtils.waitForCondition(() -> authCache.status(0) == MetadataStoreStatus.INITIALIZED,
        "Auth store not initialized");
    authStore.configureDelays(Long.MAX_VALUE, Long.MAX_VALUE); // Don't complete produce/consume
    CompletionStage<Void> stage = authWriter.addRoleAssignment(bob, "Operator", "testOrg/clusterA");

    authStore.addNewGenerationStatusRecord(2);
    verifyFailure(stage, NotMasterWriterException.class);
  }

  private Collection<Resource> rbacResources(KafkaPrincipal principal, String role, String scope) {
    RoleAssignmentValue assignment =
        (RoleAssignmentValue) authCache.get(new RoleAssignmentKey(principal, role, scope));
    return assignment == null ? null : assignment.resources();
  }

  private Collection<Resource> resources(String topic, String consumerGroup) {
    return Utils.mkSet(topicResource(topic), groupResource(consumerGroup));
  }

  private Resource topicResource(String topic) {
    return new Resource("Topic", topic, PatternType.LITERAL);
  }

  private Resource groupResource(String group) {
    return new Resource("Group", group, PatternType.LITERAL);
  }

  private void verifyFailure(CompletionStage<Void> stage, Class<? extends Exception> exceptionClass) throws Exception {
    try {
      stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertTrue("Unexpected exception " + cause, exceptionClass.isInstance(cause));
    }
  }
}
