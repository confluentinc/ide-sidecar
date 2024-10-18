package io.confluent.idesidecar.restapi.messageviewer.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@RegisterForReflection
public record SimpleConsumeMultiPartitionRequest(
    @JsonProperty("offsets") List<PartitionOffset> partitionOffsets,
    @JsonProperty("max_poll_records") Integer maxPollRecords,
    Long timestamp,
    @JsonProperty("fetch_max_bytes") Integer fetchMaxBytes,
    @JsonProperty("message_max_bytes") Integer messageMaxBytes,
    @JsonProperty("from_beginning") Boolean fromBeginning
) {
  @RegisterForReflection
  public record PartitionOffset(
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("offset") long offset
  ) {}

  public SimpleConsumeMultiPartitionRequest withMaxPollRecords(Integer maxPollRecords) {
    return new SimpleConsumeMultiPartitionRequest(
        partitionOffsets, maxPollRecords, timestamp, fetchMaxBytes, messageMaxBytes, fromBeginning
    );
  }

  public SimpleConsumeMultiPartitionRequest withFetchMaxBytes(Integer fetchMaxBytes) {
    return new SimpleConsumeMultiPartitionRequest(
        partitionOffsets, maxPollRecords, timestamp, fetchMaxBytes, messageMaxBytes, fromBeginning
    );
  }

  public SimpleConsumeMultiPartitionRequest withFromBeginning(Boolean fromBeginning) {
    return new SimpleConsumeMultiPartitionRequest(
        partitionOffsets, maxPollRecords, timestamp, fetchMaxBytes, messageMaxBytes, fromBeginning
    );
  }

  public SimpleConsumeMultiPartitionRequest withPartitionOffsets(
      List<PartitionOffset> partitionOffsets
  ) {
    return new SimpleConsumeMultiPartitionRequest(
        partitionOffsets, maxPollRecords, timestamp, fetchMaxBytes, messageMaxBytes, fromBeginning
    );
  }

  public SimpleConsumeMultiPartitionRequest withMessageMaxBytes(Integer messageMaxBytes) {
    return new SimpleConsumeMultiPartitionRequest(
        partitionOffsets, maxPollRecords, timestamp, fetchMaxBytes, messageMaxBytes, fromBeginning
    );
  }

  public SimpleConsumeMultiPartitionRequest() {
    this(null, null, null, null, null, null);
  }

}
