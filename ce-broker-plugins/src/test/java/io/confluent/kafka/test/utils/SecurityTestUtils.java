// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.test.utils;

import java.io.File;
import java.util.Set;
import javax.security.auth.login.Configuration;
import kafka.admin.ConfigCommand;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.authenticator.LoginManager;
import scala.collection.JavaConversions;

public class SecurityTestUtils {

  public static String createScramUser(String zkConnect, String userName, String password) {
    String credentials = String.format("SCRAM-SHA-256=[iterations=4096,password=%s]", password);
    String[] args = {
        "--zookeeper", zkConnect,
        "--alter", "--add-config", credentials,
        "--entity-type", "users",
        "--entity-name", userName
    };
    ConfigCommand.main(args);
    return password;
  }

  public static String scramSaslJaasConfig(String username, String password) {
    return "org.apache.kafka.common.security.scram.ScramLoginModule required\n"
        + "username=\"" + username + "\"\n"
        + "password=\"" + password + "\";\n";
  }

  public static String gssapiSaslJaasConfig(File keytabFile, String principal, String serviceName) {
    StringBuilder builder = new StringBuilder();
    builder.append("com.sun.security.auth.module.Krb5LoginModule required\n");
    builder.append("debug=true\n");
    if (serviceName != null) {
      builder.append("serviceName=\"");
      builder.append(serviceName);
      builder.append("\"\n");
    }
    builder.append("keyTab=\"" + keytabFile.getAbsolutePath() + "\"\n");
    builder.append("principal=\"");
    builder.append(principal);
    builder.append("\"\n");
    builder.append("storeKey=\"true\"\n");
    builder.append("useKeyTab=\"true\";\n");
    return builder.toString();
  }

  public static void clearSecurityConfigs() {
    System.getProperties().stringPropertyNames().stream()
        .filter(name -> name.startsWith("java.security.krb5"))
        .forEach(System::clearProperty);
    LoginManager.closeAll();
    Configuration.setConfiguration(null);
  }

  public static String[] clusterAclArgs(String zkConnect, KafkaPrincipal principal, String op) {
    return new String[] {
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--cluster",
        "--operation=" + op,
        "--allow-principal=" + principal
    };
  }

  public static String[] topicBrokerReadAclArgs(String zkConnect, KafkaPrincipal principal) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--topic=*",
        "--operation=Read",
        "--allow-principal=" + principal
    };
  }

  public static String[] produceAclArgs(String zkConnect, KafkaPrincipal principal, String topic,
      PatternType patternType) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--resource-pattern-type=" + patternType.name(),
        "--topic=" + topic,
        "--producer",
        "--allow-principal=" + principal
    };
  }

  public static String[] consumeAclArgs(String zkConnect, KafkaPrincipal principal,
      String topic, String consumerGroup, PatternType patternType) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--resource-pattern-type=" + patternType.name(),
        "--topic=" + topic,
        "--group=" + consumerGroup,
        "--consumer",
        "--allow-principal=" + principal
    };
  }

  public static String[] addConsumerGroupAclArgs(String zkConnect, KafkaPrincipal principal,
      String consumerGroup, Operation op, PatternType patternType) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--resource-pattern-type=" + patternType.name(),
        "--group=" + consumerGroup,
        "--operation=" + op.name(),
        "--allow-principal=" + principal
    };
  }

  public static String[] addTopicAclArgs(String zkConnect, KafkaPrincipal principal,
      String topic, Operation op, PatternType patternType) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--add",
        "--resource-pattern-type=" + patternType.name(),
        "--topic=" + topic,
        "--operation=" + op.name(),
        "--allow-principal=" + principal
    };
  }

  public static String[] deleteTopicAclArgs(String zkConnect, KafkaPrincipal principal,
      String topic, String op) {
    return new String[]{
        "--authorizer-properties", "zookeeper.connect=" + zkConnect,
        "--remove",
        "--force",
        "--topic=" + topic,
        "--operation=" + op,
        "--allow-principal=" + principal
    };
  }

  public static void waitForAclUpdate(Authorizer authorizer, Resource resource,
      Operation op, boolean deleted) {
    try {
      org.apache.kafka.test.TestUtils.waitForCondition(() -> {
        Set<Acl> acls = JavaConversions.setAsJavaSet(authorizer.getAcls(resource));
        boolean matches = acls.stream().anyMatch(acl -> acl.operation().equals(op));
        return deleted != matches;
      }, "ACLs not updated");
    } catch (InterruptedException e) {
      throw new RuntimeException("Wait was interrupted", e);
    }
  }
}
