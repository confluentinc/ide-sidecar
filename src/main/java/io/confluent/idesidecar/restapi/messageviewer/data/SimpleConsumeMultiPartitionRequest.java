package io.confluent.idesidecar.restapi.messageviewer.data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.soabase.recordbuilder.core.RecordBuilder;
import java.util.List;

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
  @RegisterForReflection
  public record PartitionOffset(
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("offset") long offset
  ) {
  }
}
