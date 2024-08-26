package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
public record LocalSchemaRegistry(
    String id,
    String uri,
    String connectionId
) implements Cluster, SchemaRegistry {

  LocalSchemaRegistry(String connectionId) {
    this(
        "local-sr",
        "http://localhost:8080",
        connectionId
    );
  }

  public LocalSchemaRegistry withConnectionId(String connectionId) {
    return new LocalSchemaRegistry(
        id,
        uri,
        connectionId
    );
  }
}
