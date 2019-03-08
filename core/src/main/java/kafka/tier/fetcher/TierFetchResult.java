/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.fetcher;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

public class TierFetchResult {
    public final Records records;
    public final Throwable exception;

    public TierFetchResult(Records records, Throwable exception) {
        this.records = records;
        this.exception = exception;
    }
    public static TierFetchResult emptyFetchResult() {
        return new TierFetchResult(MemoryRecords.EMPTY, null);
    }
}
