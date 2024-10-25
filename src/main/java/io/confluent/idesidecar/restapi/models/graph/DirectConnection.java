package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
public class DirectConnection extends Connection {

  public DirectConnection(
      String id,
      String name
  ) {
    super(id, name, ConnectionType.DIRECT);
  }
}
