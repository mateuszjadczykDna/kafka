/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import kafka.server.Defaults;
import kafka.server.KafkaConfig;

public class TierFetcherConfig {
    public final int numFetchThreads;

    public TierFetcherConfig(KafkaConfig config) {
        this.numFetchThreads = config.tierFetcherNumThreads();
    }

    public TierFetcherConfig(int numFetchThreads) {
        this.numFetchThreads = numFetchThreads;
    }

    public TierFetcherConfig() {
        this.numFetchThreads = Defaults.TierFetcherNumThreads();
    }
}
