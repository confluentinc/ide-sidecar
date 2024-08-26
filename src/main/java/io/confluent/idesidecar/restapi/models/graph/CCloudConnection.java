package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
public class CCloudConnection extends Connection {

  public CCloudConnection(
      String id,
      String name
  ) {
    super(id, name, ConnectionType.CCLOUD);
  }
}
