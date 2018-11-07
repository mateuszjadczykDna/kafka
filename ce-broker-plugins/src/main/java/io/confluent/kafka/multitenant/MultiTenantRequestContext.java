// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.metrics.TenantMetrics;
import io.confluent.kafka.multitenant.quota.TenantPartitionAssignor;
import io.confluent.kafka.multitenant.schema.MultiTenantApis;
import io.confluent.kafka.multitenant.schema.TenantContext;
import io.confluent.kafka.multitenant.schema.TransformableType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
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
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsRequest.PartitionDetails;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestInternals;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTenantRequestContext extends RequestContext {
  private static final Logger log = LoggerFactory.getLogger(MultiTenantRequestContext.class);

  private final TenantContext tenantContext;
  private static final int BASE_HEADER_SIZE;

  private final Metrics metrics;
  private final TenantMetrics tenantMetrics;
  private final TenantPartitionAssignor partitionAssignor;
  private final Time time;
  private final long startNanos;
  private boolean isMetadataFetchForAllTopics;
  private PatternType describeAclsPatternType;
  private boolean requestParsingFailed = false;
  private ApiException tenantApiException;

  static {
    // Actual header size is this base size + length of client-id
    BASE_HEADER_SIZE = new RequestHeader(ApiKeys.PRODUCE, (short) 0, "", 0).toStruct().sizeOf();
  }

  public MultiTenantRequestContext(RequestHeader header,
                                   String connectionId,
                                   InetAddress clientAddress,
                                   KafkaPrincipal principal,
                                   ListenerName listenerName,
                                   SecurityProtocol securityProtocol,
                                   Time time,
                                   Metrics metrics,
                                   TenantMetrics tenantMetrics,
                                   TenantPartitionAssignor partitionAssignor) {
    super(header, connectionId, clientAddress, principal, listenerName, securityProtocol);

    if (!(principal instanceof MultiTenantPrincipal)) {
      throw new IllegalArgumentException("Unexpected principal type " + principal);
    }

    this.tenantContext = new TenantContext((MultiTenantPrincipal) principal);
    this.metrics = metrics;
    this.tenantMetrics = tenantMetrics;
    this.partitionAssignor = partitionAssignor;
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
    if (!MultiTenantApis.isApiAllowed(header.apiKey())) {
      tenantApiException = Errors.CLUSTER_AUTHORIZATION_FAILED.exception();
    }

    try {
      TransformableType<TenantContext> schema = MultiTenantApis.requestSchema(api, apiVersion);
      Struct struct = (Struct) schema.read(buffer, tenantContext);
      AbstractRequest body = AbstractRequest.parseRequest(api, apiVersion, struct);
      try {
        if (body instanceof MetadataRequest) {
          isMetadataFetchForAllTopics = ((MetadataRequest) body).isAllTopics();
        } else if (body instanceof CreateAclsRequest) {
          body = transformCreateAclsRequest((CreateAclsRequest) body);
        } else if (body instanceof DescribeAclsRequest) {
          body = transformDescribeAclsRequest((DescribeAclsRequest) body);
        } else if (body instanceof DeleteAclsRequest) {
          body = transformDeleteAclsRequest((DeleteAclsRequest) body);
        } else if (body instanceof CreateTopicsRequest && partitionAssignor != null) {
          body = transformCreateTopicsRequest((CreateTopicsRequest) body, apiVersion);
        } else if (body instanceof CreatePartitionsRequest && partitionAssignor != null) {
          body = transformCreatePartitionsRequest((CreatePartitionsRequest) body, apiVersion);
        } else if (body instanceof ProduceRequest) {
          updatePartitionBytesInMetrics((ProduceRequest) body);
        }
      } catch (InvalidRequestException e) {
        // We couldn't transform the request. Save the tenant request exception and intercept later
        tenantApiException = e;
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

  @Override
  public boolean shouldIntercept() {
    return tenantApiException != null;
  }

  @Override
  public AbstractResponse intercept(AbstractRequest request, int throttleTimeMs) {
    return request.getErrorResponse(throttleTimeMs, tenantApiException);
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
      updatePartitionBytesOutMetrics((FetchResponse) body);
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
      } else if (body instanceof DescribeAclsResponse) {
        filteredResponse = filteredDescribeAclsResponse((DescribeAclsResponse) body);
      } else if (body instanceof DeleteAclsResponse) {
        filteredResponse = transformDeleteAclsResponse((DeleteAclsResponse) body);
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

  private AbstractRequest transformCreateTopicsRequest(CreateTopicsRequest topicsRequest,
                                                       short version) {
    Map<String, CreateTopicsRequest.TopicDetails> topics = topicsRequest.topics();

    Map<String, CreateTopicsRequest.TopicDetails> transformedTopics = new HashMap<>();
    Map<String, TenantPartitionAssignor.TopicInfo> topicInfos = new HashMap<>();
    for (Map.Entry<String, CreateTopicsRequest.TopicDetails> entry : topics.entrySet()) {
      CreateTopicsRequest.TopicDetails topicDetails = entry.getValue();
      int partitions = topicDetails.numPartitions;
      short replication = topicDetails.replicationFactor;
      if (!topicDetails.replicasAssignments.isEmpty()) {
        log.debug("Overriding replica assignments provided in CreateTopicsRequest");
        partitions = topicDetails.replicasAssignments.size();
        replication = (short) topicDetails.replicasAssignments.get(0).size();
      }
      if (partitions <= 0) {
        throw new InvalidRequestException("Invalid partition count " + partitions);
      }
      if (replication <= 0) {
        throw new InvalidRequestException("Invalid replication factor " + replication);
      }
      String topic = entry.getKey();
      topicInfos.put(topic, new TenantPartitionAssignor.TopicInfo(partitions, replication, 0));
    }

    String tenant = tenantContext.principal.tenantMetadata().tenantName;
    Map<String, List<List<Integer>>> assignments =
        partitionAssignor.assignPartitionsForNewTopics(tenant, topicInfos);

    for (Map.Entry<String, List<List<Integer>>> entry : assignments.entrySet()) {
      String topic = entry.getKey();
      List<List<Integer>> assignment = entry.getValue();
      Map<Integer, List<Integer>> topicAssignment = new HashMap<>(assignment.size());
      for (int i = 0; i < assignment.size(); i++) {
        topicAssignment.put(i, assignment.get(i));
      }

      Map<String, String> configs = topics.get(topic).configs;
      CreateTopicsRequest.TopicDetails topicDetails;
      if (!topicAssignment.isEmpty()) {
        topicDetails = new CreateTopicsRequest.TopicDetails(topicAssignment, configs);
      } else {
        TenantPartitionAssignor.TopicInfo topicInfo = topicInfos.get(topic);
        topicDetails = new CreateTopicsRequest.TopicDetails(topicInfo.totalPartitions(),
            topicInfo.replicationFactor(), configs);
      }
      transformedTopics.put(topic, topicDetails);
    }

    return new CreateTopicsRequest.Builder(transformedTopics, topicsRequest.timeout(),
        topicsRequest.validateOnly()).build(version);
  }

  private AbstractRequest transformCreatePartitionsRequest(
          CreatePartitionsRequest partitionsRequest, short version) {

    Map<String, PartitionDetails> partitions = partitionsRequest.newPartitions();
    Map<String, PartitionDetails> transformedPartitions = new HashMap<>();
    Map<String, Integer> totalPartitions = new HashMap<>();
    for (Map.Entry<String, PartitionDetails> entry : partitions.entrySet()) {
      String topic = entry.getKey();
      PartitionDetails newPartitionInfo = entry.getValue();
      totalPartitions.put(topic, newPartitionInfo.totalCount());
      List<List<Integer>> assignment = newPartitionInfo.newAssignments();
      if (assignment != null && !assignment.isEmpty()) {
        log.debug("Overriding replica assignments provided in CreatePartitionsRequest");
      }
    }

    String tenant = tenantContext.principal.tenantMetadata().tenantName;
    Map<String, List<List<Integer>>> assignments =
        partitionAssignor.assignPartitionsForExistingTopics(tenant, totalPartitions);

    for (Map.Entry<String, List<List<Integer>>> entry : assignments.entrySet()) {
      String topic = entry.getKey();
      List<List<Integer>> assignment = entry.getValue();
      int totalCount = partitions.get(topic).totalCount();
      transformedPartitions.put(entry.getKey(), new PartitionDetails(totalCount, assignment));
    }

    return new CreatePartitionsRequest.Builder(transformedPartitions,
        partitionsRequest.timeout(), partitionsRequest.validateOnly()).build(version);
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
      Set<DescribeConfigsResponse.ConfigEntry> filteredEntries = config.entries().stream()
          .filter(ce -> {
            if (resource.type() == ConfigResource.Type.BROKER) {
              return MultiTenantConfigRestrictions.VISIBLE_BROKER_CONFIGS.contains(ce.name());
            }
            if (resource.type() == ConfigResource.Type.TOPIC) {
              return MultiTenantConfigRestrictions.UPDATABLE_TOPIC_CONFIGS.contains(ce.name());
            }
            return false;
          })
          .collect(Collectors.toSet());
      filteredConfigs.put(
          resource,
          new DescribeConfigsResponse.Config(config.error(), filteredEntries));
    }
    return new DescribeConfigsResponse(response.throttleTimeMs(), filteredConfigs);
  }

  private DescribeAclsResponse filteredDescribeAclsResponse(DescribeAclsResponse response) {
    String tenantPrefix = tenantContext.prefix();
    List<AclBinding> aclBindings = response.acls().stream()
        .filter(binding -> {
          ResourcePattern pattern = binding.pattern();
          if (describeAclsPatternType == PatternType.LITERAL
              && pattern.patternType() != PatternType.LITERAL
              && !pattern.name().equals(tenantPrefix)) {
            return false;
          }
          if (describeAclsPatternType == PatternType.PREFIXED
              && pattern.patternType() == PatternType.PREFIXED
              && pattern.name().equals(tenantPrefix)) {
            return false;
          }
          return pattern.name().startsWith(tenantPrefix);
        })
        .map(binding -> {
          ResourcePattern pattern = binding.pattern();
          if (pattern.name().equals(tenantPrefix)) {
            ResourcePattern transformedPattern = new ResourcePattern(pattern.resourceType(),
                tenantPrefix + "*", PatternType.LITERAL);
            return new AclBinding(transformedPattern, binding.entry());
          } else {
            return binding;
          }
        }).collect(Collectors.toList());
    return new DescribeAclsResponse(response.throttleTimeMs(), response.error(), aclBindings);
  }

  private DeleteAclsResponse transformDeleteAclsResponse(DeleteAclsResponse response) {
    String tenantPrefix = tenantContext.prefix();
    List<DeleteAclsResponse.AclFilterResponse> responses = response.responses().stream().map(r -> {
      List<DeleteAclsResponse.AclDeletionResult> deletions = r.deletions().stream().map(d -> {
        ResourcePattern pattern = d.acl().pattern();
        if (pattern.name().equals(tenantPrefix)) {
          pattern = new ResourcePattern(pattern.resourceType(),
              tenantPrefix + "*", PatternType.LITERAL);
        }
        AclBinding acl = new AclBinding(pattern, d.acl().entry());
        return new DeleteAclsResponse.AclDeletionResult(d.error(), acl);
      }).collect(Collectors.toList());
      return new DeleteAclsResponse.AclFilterResponse(r.error(), deletions);
    }).collect(Collectors.toList());
    return new DeleteAclsResponse(response.throttleTimeMs(), responses);
  }

  private boolean isUnsupportedApiVersionsRequest() {
    return header.apiKey() == ApiKeys.API_VERSIONS
        && !ApiKeys.API_VERSIONS.isVersionSupported(header.apiVersion());
  }

  private short minAclsRequestVersion(AbstractRequest request) {
    return request.version() >= 1 ? request.version() : 1;
  }

  /**
   * CreateAclsRequest transformations:
   *   Principal (done as schema transformation):
   *     User:userId -> TenantUser:clusterId_userId
   *     User:* -> TenantUser*:clusterId_ (MultiTenantAuthorizer handles this)
   *   Resource (prefixing done as schema transformation, others done here):
   *     LITERAL name -> LITERAL clusterId_name
   *     LITERAL * -> PREFIXED clusterId_
   *     PREFIXED prefix -> PREFIXED clusterId_prefix
   */
  private AbstractRequest transformCreateAclsRequest(CreateAclsRequest request) {
    String prefixedWildcard = tenantContext.prefixedWildcard();
    List<CreateAclsRequest.AclCreation> aclCreations = request.aclCreations();
    List<CreateAclsRequest.AclCreation> transformedAcls = aclCreations.stream().map(creation -> {
      AclBinding acl = creation.acl();
      AclBinding transformedAcl = acl;
      ResourcePattern pattern = acl.pattern();
      ensureResourceNameNonEmpty(pattern.name());
      ensureSupportedResourceType(pattern.resourceType());
      ensureValidRequestPatternType(pattern.patternType());
      ensureValidPrincipal(acl.entry().principal());
      if (pattern.patternType() == PatternType.LITERAL
          && prefixedWildcard.equals(pattern.name())) {
        ResourcePattern prefixed = new ResourcePattern(pattern.resourceType(),
            tenantContext.prefix(), PatternType.PREFIXED);
        transformedAcl = new AclBinding(prefixed, acl.entry());
      }
      return new CreateAclsRequest.AclCreation(transformedAcl);
    }).collect(Collectors.toList());
    return new CreateAclsRequest.Builder(transformedAcls).build(minAclsRequestVersion(request));
  }

  private AbstractRequest transformDescribeAclsRequest(DescribeAclsRequest request) {
    this.describeAclsPatternType = request.filter().patternFilter().patternType();
    AclBindingFilter transformedFilter = transformAclFilter(request.filter());
    ensureValidPrincipal(request.filter().entryFilter().principal());
    return new DescribeAclsRequest.Builder(transformedFilter).build(minAclsRequestVersion(request));
  }

  private AbstractRequest transformDeleteAclsRequest(DeleteAclsRequest request) {
    List<AclBindingFilter> transformedFilters = request.filters().stream()
        .map(this::transformAclFilter)
        .collect(Collectors.toList());
    request.filters().forEach(filter -> ensureValidPrincipal(filter.entryFilter().principal()));
    return new DeleteAclsRequest.Builder(transformedFilters)
        .build(minAclsRequestVersion(request));
  }

  private void ensureResourceNameNonEmpty(String name) {
    if (tenantContext.prefix().equals(name)) {
      throw new InvalidRequestException("Invalid empty resource name specified");
    }
  }

  private void ensureSupportedResourceType(ResourceType resourceType) {
    if (resourceType != ResourceType.TOPIC && resourceType != ResourceType.GROUP
        && resourceType != ResourceType.CLUSTER && resourceType != ResourceType.TRANSACTIONAL_ID
        && resourceType != ResourceType.ANY) {
      throw new InvalidRequestException("Unsupported resource type specified: " + resourceType);
    }
  }

  private void ensureValidRequestPatternType(PatternType patternType) {
    if (patternType.isTenantPrefixed()) {
      throw new InvalidRequestException("Unsupported pattern type specified: " + patternType);
    }
  }

  private void ensureValidPrincipal(String principal) {
    try {
      if (principal != null) { // null principals are supported in filters
        SecurityUtils.parseKafkaPrincipal(principal);
      }
    } catch (IllegalArgumentException e) {
      throw new InvalidRequestException(e.getMessage());
    }
  }

  /**
   * ACL filter transformations for DescribeAclsRequest and DeleteAclRequest:
   *   Principal (done as schema transformation):
   *     User:userId -> TenantUser:clusterId_userId
   *     * -> TenantUser*:clusterId_
   *     null -> not transformed (this is ok since resource names are prefixed)
   *   Resource filter (prefixing done as schema transformation, others done here):
   *     LITERAL name -> LITERAL clusterId_name
   *     LITERAL * -> PREFIXED clusterId_
   *     LITERAL null -> CONFLUENT_ALL_TENANT_LITERAL clusterId_
   *     PREFIXED prefix -> PREFIXED clusterId_prefix
   *     PREFIXED null -> CONFLUENT_ALL_TENANT_PREFIXED clusterId_
   *     ANY name -> ANY clusterId_name
   *     ANY * -> PREFIXED clusterId_
   *     ANY null -> CONFLUENT_ALL_TENANT_ANY clusterId_
   *     MATCH name -> CONFLUENT_ONLY_TENANT_MATCH clusterId_name
   *     MATCH null -> CONFLUENT_ALL_TENANT_ANY clusterId_
   */
  private AclBindingFilter transformAclFilter(AclBindingFilter aclFilter) {
    ResourcePatternFilter pattern = aclFilter.patternFilter();
    String resourceName = pattern.name();
    PatternType patternType = pattern.patternType();
    ensureValidRequestPatternType(patternType);
    ensureResourceNameNonEmpty(resourceName);
    ensureSupportedResourceType(pattern.resourceType());

    String prefixedWildcard = tenantContext.addTenantPrefix("*");
    if (prefixedWildcard.equals(resourceName) && patternType != PatternType.PREFIXED) {
      resourceName = tenantContext.prefix();
      patternType = PatternType.PREFIXED;
    }
    if (resourceName == null) {
      switch (patternType) {
        case LITERAL:
          patternType = PatternType.CONFLUENT_ALL_TENANT_LITERAL;
          break;
        case PREFIXED:
          patternType = PatternType.CONFLUENT_ALL_TENANT_PREFIXED;
          break;
        case ANY:
        case MATCH:
          patternType = PatternType.CONFLUENT_ALL_TENANT_ANY;
          break;
        default:
          break;
      }
      resourceName = tenantContext.prefix();
    } else if (patternType == PatternType.MATCH) {
      patternType = PatternType.CONFLUENT_ONLY_TENANT_MATCH;
    }
    ResourcePatternFilter transformedPattern = new ResourcePatternFilter(
          pattern.resourceType(),
          resourceName,
          patternType);
    return new AclBindingFilter(transformedPattern, aclFilter.entryFilter());
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

  private void updatePartitionBytesInMetrics(ProduceRequest request) {
    request.partitionRecordsOrFail().entrySet().forEach(entry -> {
      TopicPartition tp = entry.getKey();
      int size = entry.getValue().sizeInBytes();
      tenantMetrics.recordPartitionBytesIn(metrics, (MultiTenantPrincipal) principal, tp, size);
    });
  }

  private void updatePartitionBytesOutMetrics(FetchResponse<?> response) {
    response.responseData().entrySet().forEach(entry -> {
      TopicPartition tp = entry.getKey();
      int size = entry.getValue().records.sizeInBytes();
      tenantMetrics.recordPartitionBytesOut(metrics, (MultiTenantPrincipal) principal, tp, size);
    });
  }
}
