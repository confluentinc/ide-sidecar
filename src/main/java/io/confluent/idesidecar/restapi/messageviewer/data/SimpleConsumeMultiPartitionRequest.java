package io.confluent.idesidecar.restapi.messageviewer.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.soabase.recordbuilder.core.RecordBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;

@JsonInclude(JsonInclude.Include.NON_NULL)
@RegisterForReflection
@RecordBuilder
public record SimpleConsumeMultiPartitionRequest(
    @JsonProperty("offsets") List<PartitionOffset> partitionOffsets,
    @JsonProperty("max_poll_records") Integer maxPollRecords,
    Long timestamp,
    @JsonProperty("fetch_max_bytes") Integer fetchMaxBytes,
    @JsonProperty("message_max_bytes") Integer messageMaxBytes,
    @JsonProperty("from_beginning") Boolean fromBeginning
) implements SimpleConsumeMultiPartitionRequestBuilder.With {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @RegisterForReflection
  public record PartitionOffset(
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("offset") long offset
  ) {
  }

  /**
   * Returns the partition offsets as a map of partition ID to offset.
   */
  public Map<Integer, Long> offsetsByPartition() {
    return Optional
        .ofNullable(partitionOffsets())
        .map(offsets -> offsets
            .stream()
            .collect(Collectors.toMap(PartitionOffset::partitionId, PartitionOffset::offset))
        )
        .orElse(null);
  }

  /**
   * Returns the consumer configuration overrides that must be applied when creating a Kafka
   * consumer for this request.
   */
  public Properties consumerConfigOverrides() {
    var props = new Properties();
    if (maxPollRecords() != null) {
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords());
    }
    if (fetchMaxBytes() != null) {
      props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes());
    }
    return props;
  }

  public String toJsonString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
