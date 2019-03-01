// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class NodeMetadataTest {

  @Test
  public void testNodeMetadata() throws Exception {
    URL http1 = new URL("http://host1:9000");
    URL http2 = new URL("http://host1:9001");
    URL http3 = new URL("http://host2:8999");
    URL https1 = new URL("https://host1:9000");
    URL https2 = new URL("https://host1:9001");

    NodeMetadata node1 = new NodeMetadata(Collections.singleton(http1));
    NodeMetadata node2 = new NodeMetadata(Collections.singleton(http1));
    assertEquals(node1, node2);
    assertEquals(0, node1.compareTo(node2));

    node1 = new NodeMetadata(Collections.singleton(http1));
    node2 = new NodeMetadata(Collections.singleton(http2));
    NodeMetadata node3 = new NodeMetadata(Collections.singleton(http3));
    assertNotEquals(node1, node2);
    assertEquals(-1, node1.compareTo(node2));
    assertEquals(1, node2.compareTo(node1));
    assertEquals(-1, node1.compareTo(node3));
    assertEquals(1, node3.compareTo(node1));

    node1 = new NodeMetadata(Collections.singleton(http1));
    node2 = new NodeMetadata(Collections.singleton(https1));
    assertEquals(1, node1.compareTo(node2));
    assertEquals(-1, node2.compareTo(node1));

    node1 = new NodeMetadata(Arrays.asList(http1, https2));
    node2 = new NodeMetadata(Arrays.asList(http2, https1));
    assertEquals(1, node1.compareTo(node2));
    assertEquals(-1, node2.compareTo(node1));

    node1 = new NodeMetadata(Collections.singleton(https2));
    node2 = new NodeMetadata(Collections.singleton(http2));
    assertEquals(-1, node1.compareTo(node2));
    assertEquals(1, node2.compareTo(node1));
  }
}
