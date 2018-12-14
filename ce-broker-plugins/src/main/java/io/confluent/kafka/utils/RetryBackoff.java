/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.utils;

import java.util.concurrent.ThreadLocalRandom;

// Exponential backoff calculation is from org.apache.kafka.clients.ClusterConnectionStates
public class RetryBackoff {

  private static final int RETRY_BACKOFF_EXP_BASE = 2;

  private final int initialBackoffMs;
  private final int maxBackoffMs;
  private final double retryBackoffMaxExp;

  public RetryBackoff(int initialBackoffMs, int maxBackoffMs) {
    this.initialBackoffMs = initialBackoffMs;
    this.maxBackoffMs = maxBackoffMs;
    this.retryBackoffMaxExp = Math.log(maxBackoffMs
        / (double) Math.max(initialBackoffMs, 1))
        / Math.log(RETRY_BACKOFF_EXP_BASE);
  }

  public int backoffMs(int attempts) {
    int backoffMs = initialBackoffMs;
    if (initialBackoffMs != maxBackoffMs) {
      double backoffExp = Math.min(attempts, retryBackoffMaxExp);
      double backoffFactor = Math.pow(RETRY_BACKOFF_EXP_BASE, backoffExp);
      backoffMs = (int) (initialBackoffMs * backoffFactor);
      // Actual backoff is randomized to avoid connection storms.
      double randomFactor = ThreadLocalRandom.current().nextDouble(0.8, 1.2);
      backoffMs = (int) (randomFactor * backoffMs);
      backoffMs =  Math.max(Math.min(backoffMs, maxBackoffMs), initialBackoffMs);
    }
    return backoffMs;
  }
}
