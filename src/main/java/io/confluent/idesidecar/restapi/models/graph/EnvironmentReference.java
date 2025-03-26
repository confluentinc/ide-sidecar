package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record EnvironmentReference(
    @JsonProperty("id") String id,
    @JsonProperty("related") String related,
    @JsonProperty("resource_name") String resourceName
) {
}
