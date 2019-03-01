// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.rbac.utils.JsonMapper;
import java.nio.ByteBuffer;
import java.util.Map;

public class MetadataServiceAssignment {

  public static final short LATEST_VERSION = 0;

  public enum AssignmentError {
    NONE((short) 0),
    DUPLICATE_URLS((short) 1);

    short errorCode;
    AssignmentError(short errorCode) {
      this.errorCode = errorCode;
    }

    public short errorCode() {
      return errorCode;
    }
  }

  private final short error;
  private final Map<String, NodeMetadata> nodes;
  private final String writerMemberId;
  private final NodeMetadata writerNodeMetadata;
  private final short version = LATEST_VERSION;

  public MetadataServiceAssignment(short error,
                                   Map<String, NodeMetadata> nodes,
                                   String writerId,
                                   NodeMetadata writerNodeMetadata) {
    this(LATEST_VERSION, error, nodes, writerId, writerNodeMetadata);
  }

  @JsonCreator
  public MetadataServiceAssignment(@JsonProperty("version") short version,
                                   @JsonProperty("error") short error,
                                   @JsonProperty("nodes") Map<String, NodeMetadata> nodes,
                                   @JsonProperty("writerMemberId") String writerMemberId,
                                   @JsonProperty("writerNodeMetadata") NodeMetadata writerNodeMetadata) {
    this.error = error;
    this.nodes = nodes;
    this.writerMemberId = writerMemberId;
    this.writerNodeMetadata = writerNodeMetadata;
  }

  @JsonProperty
  public short error() {
    return error;
  }

  @JsonProperty
  public Map<String, NodeMetadata> nodes() {
    return nodes;
  }

  @JsonProperty
  public String writerMemberId() {
    return writerMemberId;
  }

  @JsonProperty
  public NodeMetadata writerNodeMetadata() {
    return writerNodeMetadata;
  }

  @JsonProperty
  public short version() {
    return version;
  }

  public ByteBuffer serialize() {
    return JsonMapper.toByteBuffer(this);
  }

  @Override
  public String toString() {
    return "MetadataServiceAssignment(" +
        "error=" + error +
        ", nodes=" + nodes +
        ", writerMemberId='" + writerMemberId + '\'' +
        ", writerNodeMetadata=" + writerNodeMetadata +
        ", version=" + version +
        ')';
  }
}
