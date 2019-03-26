// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RetryBackoffTest {

  @Test
  public void fixedBackoff() {
    RetryBackoff backoff = new RetryBackoff(10, 10);
    for (int i = 0; i < 5; i++) {
      assertEquals(10, backoff.backoffMs(i));
    }
  }

  @Test
  public void exponentialBackoff() {
    RetryBackoff backoff = new RetryBackoff(10, 100);
    int prevBackoffMs = 0;
    for (int i = 0; i < 5; i++) {
      int backoffMs = backoff.backoffMs(i);
      assertTrue(String.format("Unexpected backoff attempts=%d backoff=%d prev=%d", i, backoffMs, prevBackoffMs),
          backoffMs >= 10 && backoffMs <= 100 && (backoffMs == 100 || backoffMs > prevBackoffMs));
    }
  }
}
