package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
public record LocalSchemaRegistry(
    String id,
    String uri,
    String connectionId
) implements SchemaRegistry {

  LocalSchemaRegistry(String connectionId, String uri) {
    this(
        "local-sr",
        uri,
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
