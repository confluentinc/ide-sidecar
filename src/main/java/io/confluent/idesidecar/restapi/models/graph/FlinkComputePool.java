package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record FlinkComputePool(
    String id,
    String displayName,
    String description,
    String identityClaim,
    String filter,
    String principal,
    String state,
    String region,
    String provider
) {
  public FlinkComputePool withConnectionId(String connectionId) {
    return new FlinkComputePool(
        id,
        displayName,
        description,
        identityClaim,
        filter,
        principal,
        state,
        region,
        provider
    );
  }
}