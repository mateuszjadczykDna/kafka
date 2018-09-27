// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.metrics.TenantMetrics;
import io.confluent.kafka.multitenant.schema.MultiTenantApis;
import io.confluent.kafka.multitenant.schema.TenantContext;
import io.confluent.kafka.multitenant.schema.TransformableType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestInternals;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiTenantRequestContext extends RequestContext {
  private final TenantContext tenantContext;
  private static final int BASE_HEADER_SIZE;
  private static final Set<String> TOPIC_CONFIG_DEFAULTS;

  private final Metrics metrics;
  private final TenantMetrics tenantMetrics;
  private final Time time;
  private final long startNanos;
  private boolean isMetadataFetchForAllTopics;
  private boolean requestParsingFailed = false;

  static {
    // Actual header size is this base size + length of client-id
    BASE_HEADER_SIZE = new RequestHeader(ApiKeys.PRODUCE, (short) 0, "", 0).toStruct().sizeOf();
    TOPIC_CONFIG_DEFAULTS = Utils.mkSet(
        "log.segment.bytes",
        "log.roll.ms",
        "log.roll.hours",
        "log.roll.jitter.ms",
        "log.roll.jitter.hours",
        "log.index.size.max.bytes",
        "log.flush.interval.messages",
        "log.flush.interval.ms",
        "log.retention.ms",
        "log.retention.minutes",
        "log.retention.hours",
        "log.retention.bytes",
        "log.index.interval.bytes",
        "message.max.bytes",
        "log.cleaner.delete.retention.ms",
        "log.cleaner.min.compaction.lag.ms",
        "log.segment.delete.delay.ms",
        "log.cleaner.min.cleanable.ratio",
        "log.cleanup.policy",
        "unclean.leader.election.enable",
        "min.insync.replicas",
        "compression.type",
        "log.preallocate",
        "log.message.format.version",
        "log.message.timestamp.type",
        "log.message.timestamp.difference.max.ms",
        "default.replication.factor",
        "num.partitions"
    );
  }

  public MultiTenantRequestContext(RequestHeader header,
                                   String connectionId,
                                   InetAddress clientAddress,
                                   KafkaPrincipal principal,
                                   ListenerName listenerName,
                                   SecurityProtocol securityProtocol,
                                   Time time,
                                   Metrics metrics,
                                   TenantMetrics tenantMetrics) {
    super(header, connectionId, clientAddress, principal, listenerName, securityProtocol);

    if (!(principal instanceof MultiTenantPrincipal)) {
      throw new IllegalArgumentException("Unexpected principal type " + principal);
    }

    this.tenantContext = new TenantContext((MultiTenantPrincipal) principal);
    this.metrics = metrics;
    this.tenantMetrics = tenantMetrics;
    this.time = time;
    this.startNanos = time.nanoseconds();
  }

  @Override
  public RequestAndSize parseRequest(ByteBuffer buffer) {
    updateRequestMetrics(buffer);
    if (isUnsupportedApiVersionsRequest()) {
      return super.parseRequest(buffer);
    }

    ApiKeys api = header.apiKey();
    short apiVersion = header.apiVersion();

    try {
      TransformableType<TenantContext> schema = MultiTenantApis.requestSchema(api, apiVersion);
      Struct struct = (Struct) schema.read(buffer, tenantContext);
      AbstractRequest body = AbstractRequest.parseRequest(api, apiVersion, struct);
      if (body instanceof MetadataRequest) {
        isMetadataFetchForAllTopics = ((MetadataRequest) body).isAllTopics();
      }
      return new RequestAndSize(body, struct.sizeOf());
    } catch (ApiException e) {
      requestParsingFailed = true;
      throw e;
    } catch (Throwable ex) {
      requestParsingFailed = true;
      throw new InvalidRequestException("Error getting request for apiKey: " + api
          + ", apiVersion: " + header.apiVersion()
          + ", connectionId: " + connectionId
          + ", listenerName: " + listenerName
          + ", principal: " + principal, ex);
    }
  }

  // Should be @Override, but this API is added in the packaging patch which is not
  // currently available while building
  public boolean shouldIntercept() {
    return !MultiTenantApis.isApiAllowed(header.apiKey());
  }

  // Should be @Override, but this API is added in the packaging patch which is not
  // currently available while building
  public AbstractResponse intercept(AbstractRequest request, int throttleTimeMs) {
    return request.getErrorResponse(throttleTimeMs,
        Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
  }

  @Override
  public Send buildResponse(AbstractResponse body) {
    if (requestParsingFailed) {
      // Since we did not successfully parse the inbound request, the response should
      // not be transformed.
      return super.buildResponse(body);
    }

    if (isUnsupportedApiVersionsRequest()) {
      Send response = super.buildResponse(body);
      updateResponseMetrics(body, response);
      return response;
    }

    ApiKeys api = header.apiKey();
    short apiVersion = header.apiVersion();
    ResponseHeader responseHeader = header.toResponseHeader();

    if (body instanceof FetchResponse) {
      // Fetch responses are unique in that they skip the usual path through the Struct object in
      // order to enable zero-copy transfer. We obviously don't want to lose this, so we do an
      // in-place transformation of the returned topic partitions.
      Send response = transformFetchResponse((FetchResponse) body, apiVersion, responseHeader);
      updateResponseMetrics(body, response);
      return response;
    } else {
      // Since the Metadata and ListGroups APIs allow users to fetch metadata for all topics or
      // groups in the cluster, we have to filter out the metadata from other tenants.
      AbstractResponse filteredResponse = body;
      if (body instanceof MetadataResponse && isMetadataFetchForAllTopics) {
        filteredResponse = filteredMetadataResponse((MetadataResponse) body);
      } else if (body instanceof ListGroupsResponse) {
        filteredResponse = filteredListGroupsResponse((ListGroupsResponse) body);
      } else if (body instanceof DescribeConfigsResponse
              && !tenantContext.principal.tenantMetadata().allowDescribeBrokerConfigs) {
        filteredResponse = filteredDescribeConfigsResponse((DescribeConfigsResponse) body);
      }

      TransformableType<TenantContext> schema = MultiTenantApis.responseSchema(api, apiVersion);
      Struct responseHeaderStruct = responseHeader.toStruct();
      Struct responseBodyStruct = RequestInternals.toStruct(filteredResponse, apiVersion);

      ByteBuffer buffer = ByteBuffer.allocate(responseHeaderStruct.sizeOf()
          + schema.sizeOf(responseBodyStruct, tenantContext));
      responseHeaderStruct.writeTo(buffer);
      schema.write(buffer, responseBodyStruct, tenantContext);
      buffer.flip();
      Send response = new NetworkSend(connectionId, buffer);
      updateResponseMetrics(body, response);
      return response;
    }
  }

  private Send transformFetchResponse(FetchResponse fetchResponse, short version,
                                      ResponseHeader header) {
    LinkedHashMap<TopicPartition, FetchResponse.PartitionData> partitionData =
        fetchResponse.responseData();
    LinkedHashMap<TopicPartition, FetchResponse.PartitionData> transformedPartitionData =
        new LinkedHashMap<>(partitionData.size());
    for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : partitionData.entrySet()) {
      TopicPartition partition = entry.getKey();
      transformedPartitionData.put(tenantContext.removeTenantPrefix(partition), entry.getValue());
    }
    FetchResponse copy = new FetchResponse(fetchResponse.error(), transformedPartitionData,
        fetchResponse.throttleTimeMs(), fetchResponse.sessionId());
    return RequestInternals.toSend(copy, version, connectionId, header);
  }

  private MetadataResponse filteredMetadataResponse(MetadataResponse response) {
    List<MetadataResponse.TopicMetadata> filteredTopics = new ArrayList<>();
    for (MetadataResponse.TopicMetadata topicMetadata : response.topicMetadata()) {
      if (tenantContext.hasTenantPrefix(topicMetadata.topic())) {
        filteredTopics.add(topicMetadata);
      }
    }

    Collection<Node> brokers = response.brokers();
    List<Node> brokersList;
    if (brokers instanceof List) {
      brokersList = (List<Node>) brokers;
    } else {
      brokersList = new ArrayList<>(brokers);
    }
    return new MetadataResponse(response.throttleTimeMs(), brokersList, response.clusterId(),
        response.controller().id(), filteredTopics);
  }

  private ListGroupsResponse filteredListGroupsResponse(ListGroupsResponse response) {
    List<ListGroupsResponse.Group> filteredGroups = new ArrayList<>();
    for (ListGroupsResponse.Group group : response.groups()) {
      if (tenantContext.hasTenantPrefix(group.groupId())) {
        filteredGroups.add(group);
      }
    }
    return new ListGroupsResponse(response.throttleTimeMs(), response.error(), filteredGroups);
  }

  private DescribeConfigsResponse filteredDescribeConfigsResponse(
                                  DescribeConfigsResponse response) {
    Map<ConfigResource, DescribeConfigsResponse.Config> configs = response.configs();
    Map<ConfigResource, DescribeConfigsResponse.Config> filteredConfigs = new HashMap<>();
    for (Map.Entry<ConfigResource, DescribeConfigsResponse.Config> entry : configs.entrySet()) {
      ConfigResource resource = entry.getKey();
      DescribeConfigsResponse.Config config = entry.getValue();
      if (resource.type() != ConfigResource.Type.BROKER) {
        filteredConfigs.put(resource, config);
      } else {
        Set<DescribeConfigsResponse.ConfigEntry> filteredEntries = new HashSet<>();
        for (DescribeConfigsResponse.ConfigEntry configEntry : config.entries()) {
          if (TOPIC_CONFIG_DEFAULTS.contains(configEntry.name())) {
            filteredEntries.add(configEntry);
          }
        }
        filteredConfigs.put(resource,
                new DescribeConfigsResponse.Config(config.error(), filteredEntries));
      }
    }
    return new DescribeConfigsResponse(response.throttleTimeMs(), filteredConfigs);
  }

  private boolean isUnsupportedApiVersionsRequest() {
    return header.apiKey() == ApiKeys.API_VERSIONS
        && !ApiKeys.API_VERSIONS.isVersionSupported(header.apiVersion());
  }

  int calculateRequestSize(ByteBuffer buffer) {
    return 4  // size field before header
        + BASE_HEADER_SIZE    // header size excluding client-id string
        + Utils.utf8Length(header.clientId())
        + buffer.remaining(); // request body
  }

  private void updateRequestMetrics(ByteBuffer buffer) {
    tenantMetrics.recordRequest(metrics, (MultiTenantPrincipal) principal, header.apiKey(),
        calculateRequestSize(buffer));
  }

  private void updateResponseMetrics(AbstractResponse body, Send response) {
    tenantMetrics.recordResponse(metrics, (MultiTenantPrincipal) principal, header.apiKey(),
        response.size(), time.nanoseconds() - startNanos, body.errorCounts());
  }

}
