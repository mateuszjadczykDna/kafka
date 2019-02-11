// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer;

import static org.junit.Assert.assertEquals;

import io.confluent.kafka.security.authorizer.provider.ProviderFailedException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.After;
import org.junit.Test;

public class EmbeddedAuthorizerTest {

  private final EmbeddedAuthorizer authorizer = new EmbeddedAuthorizer();
  private final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");
  private final KafkaPrincipal group = new KafkaPrincipal(AccessRule.GROUP_PRINCIPAL_TYPE, "groupA");
  private final Resource topic = new Resource(new ResourceType("Topic"), "testTopic", PatternType.LITERAL);
  private final String scope = "testScope";

  @After
  public void tearDown() {
    TestGroupProvider.reset();
    TestAccessRuleProvider.reset();
  }

  @Test
  public void testAccessRuleProvider() {
    configureAuthorizer("TEST", "NONE");
    verifyAccessRules(principal, principal);
  }

  @Test
  public void testGroupProvider() {
    configureAuthorizer("TEST", "TEST");
    TestGroupProvider.groups.put(principal, Collections.singleton(group));
    verifyAccessRules(principal, group);

    TestGroupProvider.groups.remove(principal);
    List<AuthorizeResult> result =
        authorizer.authorize(principal, "localhost", Arrays.asList(action("Write"), action("Read"), action("Alter")));
    assertEquals(Arrays.asList(AuthorizeResult.DENIED, AuthorizeResult.DENIED, AuthorizeResult.DENIED), result);
  }

  private void verifyAccessRules(KafkaPrincipal userPrincipal, KafkaPrincipal rulePrincipal) {

    Action write = action("Write");
    List<AuthorizeResult> result;
    result = authorizer.authorize(userPrincipal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(AuthorizeResult.DENIED), result);

    Action read = action("Read");
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(read, write));
    assertEquals(Arrays.asList(AuthorizeResult.DENIED, AuthorizeResult.DENIED), result);

    Set<AccessRule> topicRules = new HashSet<>();
    TestAccessRuleProvider.accessRules.put(topic, topicRules);
    topicRules.add(new AccessRule(rulePrincipal, PermissionType.ALLOW, "localhost", read.operation(), ""));

    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(read, write));
    assertEquals(Arrays.asList(AuthorizeResult.ALLOWED, AuthorizeResult.DENIED), result);
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(write, read));
    assertEquals(Arrays.asList(AuthorizeResult.DENIED, AuthorizeResult.ALLOWED), result);

    topicRules.add(new AccessRule(rulePrincipal, PermissionType.ALLOW, "localhost", write.operation(), ""));
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(write, read));
    assertEquals(Arrays.asList(AuthorizeResult.ALLOWED, AuthorizeResult.ALLOWED), result);

    Action alter = action("Alter");
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(write, read, alter));
    assertEquals(Arrays.asList(AuthorizeResult.ALLOWED, AuthorizeResult.ALLOWED, AuthorizeResult.DENIED), result);
    TestAccessRuleProvider.superUsers.add(rulePrincipal);
    result = authorizer.authorize(userPrincipal, "localhost", Arrays.asList(write, read, alter));
    assertEquals(Arrays.asList(AuthorizeResult.ALLOWED, AuthorizeResult.ALLOWED, AuthorizeResult.ALLOWED), result);
  }

  @Test
  public void testAccessRuleProviderFailure() {
    configureAuthorizer("TEST", "NONE");
    TestAccessRuleProvider.exception = new ProviderFailedException("Provider failed");
    verifyProviderFailure(AuthorizeResult.AUTHORIZER_FAILED);
  }

  @Test
  public void testGroupProviderFailure() {
    configureAuthorizer("TEST", "TEST");
    TestGroupProvider.exception = new ProviderFailedException("Provider failed");
    verifyProviderFailure(AuthorizeResult.AUTHORIZER_FAILED);
  }

  @Test
  public void testUnexpectedException() {
    configureAuthorizer("TEST", "NONE");
    TestAccessRuleProvider.exception = new RuntimeException("Unknown failure");
    verifyProviderFailure(AuthorizeResult.UNKNOWN_ERROR);
  }

  private void verifyProviderFailure(AuthorizeResult expectedResult) {
    Action write = action("Write");
    List<AuthorizeResult> result;
    result = authorizer.authorize(principal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(expectedResult), result);

    TestAccessRuleProvider.superUsers.add(principal);
    result = authorizer.authorize(principal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(expectedResult), result);
  }

  @Test
  public void testInvalidScope() {
    configureAuthorizer("TEST", "NONE");

    Action write = new Action("someScope", topic.resourceType(), topic.name(), new Operation("Write"));
    List<AuthorizeResult> result;
    result = authorizer.authorize(principal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(AuthorizeResult.UNKNOWN_SCOPE), result);

    TestAccessRuleProvider.superUsers.add(principal);
    result = authorizer.authorize(principal, "localhost", Collections.singletonList(write));
    assertEquals(Collections.singletonList(AuthorizeResult.UNKNOWN_SCOPE), result);
  }

  private void configureAuthorizer(String accessRuleProvider, String groupProvider) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP, accessRuleProvider);
    props.put(ConfluentAuthorizerConfig.GROUP_PROVIDER_PROP, groupProvider);
    authorizer.configure(props);
  }

  private Action action(String operation) {
    return new Action(scope, topic.resourceType(), topic.name(), new Operation(operation));
  }

}
