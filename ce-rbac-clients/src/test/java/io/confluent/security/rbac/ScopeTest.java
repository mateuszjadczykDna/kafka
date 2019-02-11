// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.confluent.security.rbac.utils.JsonMapper;
import io.confluent.security.test.utils.JsonTestUtils;
import org.junit.Test;

public class ScopeTest {

  @Test
  public void testScopes() throws Exception {
    Scope abc = scope("{ \"name\": \"a/b/c\" }");
    Scope ab = scope("{ \"name\": \"a/b\" }");
    Scope a = scope("{ \"name\": \"a\" }");
    Scope c = scope("{ \"name\": \"c\" }");

    assertEquals(abc, scope(JsonMapper.objectMapper().writeValueAsString(abc)));
    assertEquals(ab, scope(JsonMapper.objectMapper().writeValueAsString(ab)));
    assertEquals(a, scope(JsonMapper.objectMapper().writeValueAsString(a)));

    assertEquals(ab, abc.parent());
    assertEquals(a, ab.parent());
    assertNull(a.parent());

    assertTrue(abc.containsScope(abc));
    assertTrue(a.containsScope(ab));
    assertTrue(a.containsScope(abc));
    assertTrue(ab.containsScope(abc));
    assertFalse(abc.containsScope(ab));
    assertFalse(abc.containsScope(a));
    assertFalse(abc.containsScope(c));
    assertFalse(ab.containsScope(c));
    assertFalse(a.containsScope(c));
  }

  private Scope scope(String json) {
    return JsonTestUtils.jsonObject(Scope.class, json);
  }
}