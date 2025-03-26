package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record FlinkComputePool(
    @JsonProperty("id") String id,
    @JsonProperty("spec") FlinkComputePoolSpec spec,
    @JsonProperty("status") FlinkComputePoolStatus status,
    @JsonProperty("connectionId") String connectionId
) {
  public FlinkComputePool withConnectionId(String connectionId) {
    return new FlinkComputePool(this.id, this.spec, this.status, connectionId);
  }
}

