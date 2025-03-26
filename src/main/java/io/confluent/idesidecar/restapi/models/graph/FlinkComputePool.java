package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record FlinkComputePool(
    @JsonProperty("id") String id,
    @JsonProperty("spec") FlinkComputePoolSpec spec,
    @JsonProperty("status") FlinkComputePoolStatus status
) {
  public FlinkComputePool withConnectionId(String connectionId) {
    return new FlinkComputePool(connectionId, this.spec, this.status);
  }
}

