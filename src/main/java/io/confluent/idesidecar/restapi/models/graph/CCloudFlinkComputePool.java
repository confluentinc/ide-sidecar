package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.eclipse.microprofile.graphql.Name;

@RegisterForReflection
public record CCloudFlinkComputePool(
    @JsonProperty("id") String id,
    @JsonProperty("display_name") String displayName,
    @JsonProperty("provider") @Name("provider") String provider,
    @JsonProperty("region") String region,
    @JsonProperty("max_cfu") int maxCfu,
    @JsonProperty("environment") CCloudReference environment,
    @JsonProperty("organization") CCloudReference organization,
    @JsonProperty("connectionId") String connectionId
) {
  public CCloudFlinkComputePool withConnectionId(String connectionId) {
    return new CCloudFlinkComputePool(
        this.id,
        this.displayName,
        this.provider,
        this.region,
        this.maxCfu,
        this.environment,
        this.organization,
        connectionId);
  }
}

