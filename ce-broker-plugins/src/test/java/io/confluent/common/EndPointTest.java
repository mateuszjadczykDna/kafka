// Copyright 2018, Confluent Inc.

package io.confluent.common;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EndPointTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testEndPointEquals() {
        EndPoint e0 = new EndPoint("foo", 8080, SecurityProtocol.PLAINTEXT);
        EndPoint e1 = new EndPoint("foo", 8080, SecurityProtocol.SASL_SSL);
        EndPoint e2 = new EndPoint("", 8080, SecurityProtocol.PLAINTEXT);
        EndPoint e3 = new EndPoint("foo", 8080, SecurityProtocol.SASL_SSL);

        checkNotEqual(e0, e1);
        checkNotEqual(e0, e2);
        checkNotEqual(e1, e2);
        checkEqual(e1, e3);
        checkNotEqual(e0, e3);
    }

    private void checkNotEqual(EndPoint a, EndPoint b) {
        assertFalse(a.equals(b));
        assertFalse(b.equals(a));
    }

    private void checkEqual(EndPoint a, EndPoint b) {
        assertEquals(a, b);
        assertEquals(b, a);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(b.hashCode(), a.hashCode());
    }

    @Test
    public void testEndPointParsing() {
        checkEndPoint(SecurityProtocol.PLAINTEXT, "", 8080, "PLAINTEXT://:8080");
        checkEndPoint(SecurityProtocol.SASL_SSL, "2001:0db8:85a3:0000:0000:8a2e:0370:7334", 23,
            "SASL_SSL://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:23");
        checkEndPoint(SecurityProtocol.SSL, "example.com", 9999, "SSL://example.com:9999");
    }

    private void checkEndPoint(SecurityProtocol securityProtocol, String host,
                               int port, String connectionString) {
        EndPoint ep0 = new EndPoint(host, port, securityProtocol);
        EndPoint ep1 = EndPoint.parse(connectionString);
        assertEquals(ep0, ep1);
        assertEquals(ep1, ep0);
        assertEquals(ep0.hashCode(), ep1.hashCode());
        assertEquals(ep1.hashCode(), ep0.hashCode());
        assertEquals(connectionString, ep0.toString());
        assertEquals(connectionString, ep1.toString());
    }

    @Test(expected = RuntimeException.class)
    public void testEndPointParsingFailsWithoutName() {
        EndPoint.parse("febtober");
    }

    @Test(expected = RuntimeException.class)
    public void testEndPointParsingFailsWithoutPort() {
        EndPoint.parse("PLAINTEXT://example");
    }
}
