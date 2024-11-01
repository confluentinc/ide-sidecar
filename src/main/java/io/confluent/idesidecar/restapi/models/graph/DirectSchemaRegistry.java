package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
public record DirectSchemaRegistry(
    String id,
    String uri,
    String connectionId
) implements SchemaRegistry {

  public DirectSchemaRegistry withConnectionId(String connectionId) {
    return new DirectSchemaRegistry(
        id,
        uri,
        connectionId
    );
  }
}
