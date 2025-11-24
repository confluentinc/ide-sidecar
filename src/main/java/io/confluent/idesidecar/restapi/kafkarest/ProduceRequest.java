package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestHeader;
import jakarta.validation.Valid;
import java.util.Date;
import java.util.List;

public record ProduceRequest(
    @JsonProperty("partition_id") Integer partitionId,
    @Valid List<@Valid ProduceRequestHeader> headers,
    @Valid ProduceRequestData key,
    @Valid ProduceRequestData value,
    Date timestamp
) {

  public ProduceRequest withKey(ProduceRequestData key) {
    return new ProduceRequest(partitionId, headers, key, value, timestamp);
  }

  public ProduceRequest withValue(ProduceRequestData value) {
    return new ProduceRequest(partitionId, headers, key, value, timestamp);
  }
}
