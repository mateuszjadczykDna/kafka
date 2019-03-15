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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.message.AppendRecordsRequestData;
import org.apache.kafka.common.message.AppendRecordsResponseData;
import org.apache.kafka.common.message.BeginEpochRequestData;
import org.apache.kafka.common.message.BeginEpochResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.ElectPreferredLeadersRequestData;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData;
import org.apache.kafka.common.message.EndEpochRequestData;
import org.apache.kafka.common.message.EndEpochResponseData;
import org.apache.kafka.common.message.FetchEndOffsetRequestData;
import org.apache.kafka.common.message.FetchEndOffsetResponseData;
import org.apache.kafka.common.message.FetchRecordsRequestData;
import org.apache.kafka.common.message.FetchRecordsResponseData;
import org.apache.kafka.common.message.FindLeaderRequestData;
import org.apache.kafka.common.message.FindLeaderResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.TierListOffsetRequestData;
import org.apache.kafka.common.message.TierListOffsetResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ControlledShutdownRequest;
import org.apache.kafka.common.requests.ControlledShutdownResponse;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteRecordsRequest;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.requests.DescribeDelegationTokenResponse;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.LeaderAndIsrResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenResponse;
import org.apache.kafka.common.requests.StopReplicaRequest;
import org.apache.kafka.common.requests.StopReplicaResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.requests.UpdateMetadataRequest;
import org.apache.kafka.common.requests.UpdateMetadataResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_BYTES;
import static org.apache.kafka.common.protocol.types.Type.RECORDS;

/**
 * Identifiers for all the Kafka APIs
 */
public enum ApiKeys {
    PRODUCE(0, "Produce", ProduceRequest.schemaVersions(), ProduceResponse.schemaVersions()),
    FETCH(1, "Fetch", FetchRequest.schemaVersions(), FetchResponse.schemaVersions()),
    LIST_OFFSETS(2, "ListOffsets", ListOffsetRequest.schemaVersions(), ListOffsetResponse.schemaVersions()),
    METADATA(3, "Metadata", MetadataRequestData.SCHEMAS, MetadataResponseData.SCHEMAS),
    LEADER_AND_ISR(4, "LeaderAndIsr", true, LeaderAndIsrRequest.schemaVersions(), LeaderAndIsrResponse.schemaVersions()),
    STOP_REPLICA(5, "StopReplica", true, StopReplicaRequest.schemaVersions(), StopReplicaResponse.schemaVersions()),
    UPDATE_METADATA(6, "UpdateMetadata", true, UpdateMetadataRequest.schemaVersions(),
            UpdateMetadataResponse.schemaVersions()),
    CONTROLLED_SHUTDOWN(7, "ControlledShutdown", true, ControlledShutdownRequest.schemaVersions(),
            ControlledShutdownResponse.schemaVersions()),
    OFFSET_COMMIT(8, "OffsetCommit", OffsetCommitRequest.schemaVersions(), OffsetCommitResponse.schemaVersions()),
    OFFSET_FETCH(9, "OffsetFetch", OffsetFetchRequest.schemaVersions(), OffsetFetchResponse.schemaVersions()),
    FIND_COORDINATOR(10, "FindCoordinator", FindCoordinatorRequest.schemaVersions(),
            FindCoordinatorResponse.schemaVersions()),
    JOIN_GROUP(11, "JoinGroup", JoinGroupRequestData.SCHEMAS, JoinGroupResponseData.SCHEMAS),
    HEARTBEAT(12, "Heartbeat", HeartbeatRequest.schemaVersions(), HeartbeatResponse.schemaVersions()),
    LEAVE_GROUP(13, "LeaveGroup", LeaveGroupRequestData.SCHEMAS, LeaveGroupResponseData.SCHEMAS),
    SYNC_GROUP(14, "SyncGroup", SyncGroupRequest.schemaVersions(), SyncGroupResponse.schemaVersions()),
    DESCRIBE_GROUPS(15, "DescribeGroups", DescribeGroupsRequestData.SCHEMAS,
            DescribeGroupsResponseData.SCHEMAS),
    LIST_GROUPS(16, "ListGroups", ListGroupsRequest.schemaVersions(), ListGroupsResponse.schemaVersions()),
    SASL_HANDSHAKE(17, "SaslHandshake", SaslHandshakeRequestData.SCHEMAS, SaslHandshakeResponseData.SCHEMAS),
    API_VERSIONS(18, "ApiVersions", ApiVersionsRequest.schemaVersions(), ApiVersionsResponse.schemaVersions()) {
        @Override
        public Struct parseResponse(short version, ByteBuffer buffer) {
            // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
            // using a version higher than that supported by the broker, a version 0 response is sent
            // to the client indicating UNSUPPORTED_VERSION.
            return parseResponse(version, buffer, (short) 0);
        }
    },
    CREATE_TOPICS(19, "CreateTopics", CreateTopicsRequestData.SCHEMAS, CreateTopicsResponseData.SCHEMAS),
    DELETE_TOPICS(20, "DeleteTopics", DeleteTopicsRequestData.SCHEMAS, DeleteTopicsResponseData.SCHEMAS),
    DELETE_RECORDS(21, "DeleteRecords", DeleteRecordsRequest.schemaVersions(), DeleteRecordsResponse.schemaVersions()),
    INIT_PRODUCER_ID(22, "InitProducerId", InitProducerIdRequest.schemaVersions(),
            InitProducerIdResponse.schemaVersions()),
    OFFSET_FOR_LEADER_EPOCH(23, "OffsetForLeaderEpoch", false, OffsetsForLeaderEpochRequest.schemaVersions(),
            OffsetsForLeaderEpochResponse.schemaVersions()),
    ADD_PARTITIONS_TO_TXN(24, "AddPartitionsToTxn", false, RecordBatch.MAGIC_VALUE_V2,
            AddPartitionsToTxnRequest.schemaVersions(), AddPartitionsToTxnResponse.schemaVersions()),
    ADD_OFFSETS_TO_TXN(25, "AddOffsetsToTxn", false, RecordBatch.MAGIC_VALUE_V2, AddOffsetsToTxnRequest.schemaVersions(),
            AddOffsetsToTxnResponse.schemaVersions()),
    END_TXN(26, "EndTxn", false, RecordBatch.MAGIC_VALUE_V2, EndTxnRequest.schemaVersions(),
            EndTxnResponse.schemaVersions()),
    WRITE_TXN_MARKERS(27, "WriteTxnMarkers", true, RecordBatch.MAGIC_VALUE_V2, WriteTxnMarkersRequest.schemaVersions(),
            WriteTxnMarkersResponse.schemaVersions()),
    TXN_OFFSET_COMMIT(28, "TxnOffsetCommit", false, RecordBatch.MAGIC_VALUE_V2, TxnOffsetCommitRequest.schemaVersions(),
            TxnOffsetCommitResponse.schemaVersions()),
    DESCRIBE_ACLS(29, "DescribeAcls", DescribeAclsRequest.schemaVersions(), DescribeAclsResponse.schemaVersions()),
    CREATE_ACLS(30, "CreateAcls", CreateAclsRequest.schemaVersions(), CreateAclsResponse.schemaVersions()),
    DELETE_ACLS(31, "DeleteAcls", DeleteAclsRequest.schemaVersions(), DeleteAclsResponse.schemaVersions()),
    DESCRIBE_CONFIGS(32, "DescribeConfigs", DescribeConfigsRequest.schemaVersions(),
            DescribeConfigsResponse.schemaVersions()),
    ALTER_CONFIGS(33, "AlterConfigs", AlterConfigsRequest.schemaVersions(),
            AlterConfigsResponse.schemaVersions()),
    ALTER_REPLICA_LOG_DIRS(34, "AlterReplicaLogDirs", AlterReplicaLogDirsRequest.schemaVersions(),
            AlterReplicaLogDirsResponse.schemaVersions()),
    DESCRIBE_LOG_DIRS(35, "DescribeLogDirs", DescribeLogDirsRequest.schemaVersions(),
            DescribeLogDirsResponse.schemaVersions()),
    SASL_AUTHENTICATE(36, "SaslAuthenticate", SaslAuthenticateRequestData.SCHEMAS,
            SaslAuthenticateResponseData.SCHEMAS),
    CREATE_PARTITIONS(37, "CreatePartitions", CreatePartitionsRequest.schemaVersions(),
            CreatePartitionsResponse.schemaVersions()),
    CREATE_DELEGATION_TOKEN(38, "CreateDelegationToken", CreateDelegationTokenRequest.schemaVersions(), CreateDelegationTokenResponse.schemaVersions()),
    RENEW_DELEGATION_TOKEN(39, "RenewDelegationToken", RenewDelegationTokenRequest.schemaVersions(), RenewDelegationTokenResponse.schemaVersions()),
    EXPIRE_DELEGATION_TOKEN(40, "ExpireDelegationToken", ExpireDelegationTokenRequest.schemaVersions(), ExpireDelegationTokenResponse.schemaVersions()),
    DESCRIBE_DELEGATION_TOKEN(41, "DescribeDelegationToken", DescribeDelegationTokenRequest.schemaVersions(), DescribeDelegationTokenResponse.schemaVersions()),
    DELETE_GROUPS(42, "DeleteGroups", DeleteGroupsRequest.schemaVersions(), DeleteGroupsResponse.schemaVersions()),
    ELECT_PREFERRED_LEADERS(43, "ElectPreferredLeaders", ElectPreferredLeadersRequestData.SCHEMAS,
            ElectPreferredLeadersResponseData.SCHEMAS),

    /* ----- Begin internal APIs: API ids decrement sequentially starting from Short.MAX_VALUE with `isInternal` set to true ----- */

    // Quorum APIs
    VOTE(900, "Vote", VoteRequestData.SCHEMAS, VoteResponseData.SCHEMAS),
    BEGIN_EPOCH(901, "BeginEpoch", BeginEpochRequestData.SCHEMAS, BeginEpochResponseData.SCHEMAS),
    END_EPOCH(902, "EndEpoch", EndEpochRequestData.SCHEMAS, EndEpochResponseData.SCHEMAS),
    FETCH_RECORDS(903, "FetchRecords", FetchRecordsRequestData.SCHEMAS, FetchRecordsResponseData.SCHEMAS),
    FETCH_END_OFFSET(904, "FetchEndOffset", FetchEndOffsetRequestData.SCHEMAS, FetchEndOffsetResponseData.SCHEMAS),
    FIND_LEADER(905, "FindLeader", FindLeaderRequestData.SCHEMAS, FindLeaderResponseData.SCHEMAS),
    APPEND_RECORDS(906, "AppendRecords", AppendRecordsRequestData.SCHEMAS, AppendRecordsResponseData.SCHEMAS),

    TIER_LIST_OFFSET(32767, "TierListOffsets", true, TierListOffsetRequestData.SCHEMAS, TierListOffsetResponseData.SCHEMAS, true);

    /* ----- End internal APIs ----- */

    private static final Map<Short, ApiKeys> ID_TO_TYPE = new HashMap<>(values().length);

    static {
        for (ApiKeys key : ApiKeys.values()) {
            ApiKeys oldValue = ID_TO_TYPE.put(key.id, key);
            if (oldValue != null)
                throw new ExceptionInInitializerError("id " + key.id + " for API key " + key.name +
                        " has already been used by " + oldValue.name);
        }
    }

    /** the permanent and immutable id of an API--this can't change ever */
    public final short id;

    /** indicates if this is a Confluent-internal API */
    public final boolean isInternal;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    /** indicates if this is a ClusterAction request used only by brokers */
    public final boolean clusterAction;

    /** indicates the minimum required inter broker magic required to support the API */
    public final byte minRequiredInterBrokerMagic;

    public final Schema[] requestSchemas;
    public final Schema[] responseSchemas;
    public final boolean requiresDelayedAllocation;

    ApiKeys(int id, String name, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, false, requestSchemas, responseSchemas);
    }

    ApiKeys(int id, String name, boolean clusterAction, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, clusterAction, RecordBatch.MAGIC_VALUE_V0, requestSchemas, responseSchemas);
    }

    ApiKeys(int id, String name, boolean clusterAction, Schema[] requestSchemas, Schema[] responseSchemas, boolean isInternal) {
        this(id, name, clusterAction, RecordBatch.MAGIC_VALUE_V0, requestSchemas, responseSchemas, isInternal);
    }

    ApiKeys(int id, String name, boolean clusterAction, byte minRequiredInterBrokerMagic,
            Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, clusterAction, minRequiredInterBrokerMagic, requestSchemas, responseSchemas, false);
    }

    ApiKeys(int id, String name, boolean clusterAction, byte minRequiredInterBrokerMagic,
            Schema[] requestSchemas, Schema[] responseSchemas, boolean isInternal) {
        if (id < 0)
            throw new IllegalArgumentException("id must not be negative, id: " + id);
        this.id = (short) id;
        this.isInternal = isInternal;
        this.name = name;
        this.clusterAction = clusterAction;
        this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;

        if (requestSchemas.length != responseSchemas.length)
            throw new IllegalStateException(requestSchemas.length + " request versions for api " + name
                    + " but " + responseSchemas.length + " response versions.");

        for (int i = 0; i < requestSchemas.length; ++i) {
            if (requestSchemas[i] == null)
                throw new IllegalStateException("Request schema for api " + name + " for version " + i + " is null");
            if (responseSchemas[i] == null)
                throw new IllegalStateException("Response schema for api " + name + " for version " + i + " is null");
        }

        boolean requestRetainsBufferReference = false;
        for (Schema requestVersionSchema : requestSchemas) {
            if (retainsBufferReference(requestVersionSchema)) {
                requestRetainsBufferReference = true;
                break;
            }
        }
        this.requiresDelayedAllocation = requestRetainsBufferReference;
        this.requestSchemas = requestSchemas;
        this.responseSchemas = responseSchemas;
    }

    public static ApiKeys forId(int id) {
        ApiKeys apiKeys = ID_TO_TYPE.get((short) id);
        if (apiKeys == null)
            throw new IllegalArgumentException(String.format("Unexpected ApiKeys id `%s`", id));
        return apiKeys;
    }

    public static boolean hasId(int id) {
        return ID_TO_TYPE.containsKey((short) id);
    }

    public short latestVersion() {
        return (short) (requestSchemas.length - 1);
    }

    public short oldestVersion() {
        return 0;
    }

    public Schema requestSchema(short version) {
        return schemaFor(requestSchemas, version);
    }

    public Schema responseSchema(short version) {
        return schemaFor(responseSchemas, version);
    }

    public Struct parseRequest(short version, ByteBuffer buffer) {
        return requestSchema(version).read(buffer);
    }

    public Struct parseResponse(short version, ByteBuffer buffer) {
        return responseSchema(version).read(buffer);
    }

    protected Struct parseResponse(short version, ByteBuffer buffer, short fallbackVersion) {
        int bufferPosition = buffer.position();
        try {
            return responseSchema(version).read(buffer);
        } catch (SchemaException e) {
            if (version != fallbackVersion) {
                buffer.position(bufferPosition);
                return responseSchema(fallbackVersion).read(buffer);
            } else
                throw e;
        }
    }

    private Schema schemaFor(Schema[] versions, short version) {
        if (!isVersionSupported(version))
            throw new IllegalArgumentException("Invalid version for API key " + this + ": " + version);
        return versions[version];
    }

    public boolean isVersionSupported(short apiVersion) {
        return apiVersion >= oldestVersion() && apiVersion <= latestVersion();
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKeys key : ApiKeys.values()) {
            if (key.isInternal)
                continue;
            b.append("<tr>\n");
            b.append("<td>");
            b.append("<a href=\"#The_Messages_" + key.name + "\">" + key.name + "</a>");
            b.append("</td>");
            b.append("<td>");
            b.append(key.id);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }

    private static boolean retainsBufferReference(Schema schema) {
        final AtomicBoolean hasBuffer = new AtomicBoolean(false);
        Schema.Visitor detector = new Schema.Visitor() {
            @Override
            public void visit(Type field) {
                if (field == BYTES || field == NULLABLE_BYTES || field == RECORDS)
                    hasBuffer.set(true);
            }
        };
        schema.walk(detector);
        return hasBuffer.get();
    }

}
