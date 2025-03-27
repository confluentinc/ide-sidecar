package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record FlinkComputePoolSpec(
    @JsonProperty("display_name") String displayName,
    @JsonProperty("cloud") String cloud,
    @JsonProperty("region") String region,
    @JsonProperty("max_cfu") int maxCfu,
    @JsonProperty("environment") CCloudEnvironment environment,
    @JsonProperty("network") NetworkReference network
) {

}
