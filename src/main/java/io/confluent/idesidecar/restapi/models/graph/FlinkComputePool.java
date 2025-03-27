package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record FlinkComputePool(
    @JsonProperty("id") String id,
    @JsonProperty("display_name") String displayName,
    @JsonProperty("cloud") String cloud,
    @JsonProperty("region") String region,
    @JsonProperty("max_cfu") int maxCfu,
    @JsonProperty("environment") CCloudReference environment,
    @JsonProperty("organization") CCloudReference organization,
    @JsonProperty("connectionId") String connectionId
) {
  public FlinkComputePool withConnectionId(String connectionId) {
    return new FlinkComputePool(
        this.id,
        this.displayName,
        this.cloud,
        this.region,
        this.maxCfu,
        this.environment,
        this.organization,
        connectionId);
  }
}

