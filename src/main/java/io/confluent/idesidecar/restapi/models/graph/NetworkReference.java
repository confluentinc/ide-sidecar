package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record NetworkReference(
    @JsonProperty("id") String id,
    @JsonProperty("environment") String environment,
    @JsonProperty("related") String related,
    @JsonProperty("resource_name") String resourceName
) {
}
