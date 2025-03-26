package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record FlinkComputePoolStatus(
    @JsonProperty("phase") String phase,
    @JsonProperty("current_cfu") int currentCfu
) {
}
