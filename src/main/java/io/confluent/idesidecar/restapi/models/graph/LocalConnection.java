package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
public class LocalConnection extends Connection {

  public LocalConnection(String id) {
    super(id, "local", ConnectionType.LOCAL);
  }
}
