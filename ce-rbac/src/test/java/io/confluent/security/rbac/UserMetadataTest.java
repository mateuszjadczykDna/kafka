// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import static org.junit.Assert.assertEquals;

import io.confluent.security.authorizer.utils.JsonMapper;
import io.confluent.security.authorizer.utils.JsonTestUtils;
import java.util.Collections;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

public class UserMetadataTest {

  @Test
  public void testUserMetadata() throws Exception {
    UserMetadata metadata1 = userMetadata("{ \"groups\": [ \"Group:Developer\" ] }");
    assertEquals(Collections.singleton(new KafkaPrincipal("Group", "Developer")), metadata1.groups());
    assertEquals(metadata1, userMetadata(JsonMapper.objectMapper().writeValueAsString(metadata1)));

    UserMetadata metadata2 = userMetadata("{ \"groups\": [ \"Group:Developer\", \"Group:Tester\" ] }");
    assertEquals(Utils.mkSet(new KafkaPrincipal("Group", "Developer"), new KafkaPrincipal("Group", "Tester")),
        metadata2.groups());
    assertEquals(metadata2, userMetadata(JsonMapper.objectMapper().writeValueAsString(metadata2)));

    UserMetadata emptyGroups = userMetadata("{ \"groups\": [] }");
    assertEquals(Collections.emptySet(), emptyGroups.groups());
    assertEquals(emptyGroups, userMetadata(JsonMapper.objectMapper().writeValueAsString(emptyGroups)));
  }

  private UserMetadata userMetadata(String json) {
    return JsonTestUtils.jsonObject(UserMetadata.class, json);
  }
}