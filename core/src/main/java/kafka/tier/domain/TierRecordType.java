/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.domain;

public enum TierRecordType {
    InitLeader(TierTopicInitLeader.ID),
    ObjectMetadata(TierObjectMetadata.ID);

    private final byte typeByte;
    TierRecordType(byte typeByte) {
        this.typeByte = typeByte;
    }
}