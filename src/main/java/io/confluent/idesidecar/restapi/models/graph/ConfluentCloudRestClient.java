package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.MultiMap;

@RegisterForReflection
public abstract class ConfluentCloudRestClient extends ConfluentRestClient {

  @Override
  protected MultiMap headersFor(String connectionId) throws ConnectionNotFoundException {
    var connectionState = connections.getConnectionState(connectionId);
    if (connectionState instanceof CCloudConnectionState cCloudConnectionState) {
      return cCloudConnectionState
          .getOauthContext()
          .getControlPlaneAuthenticationHeaders();
    } else {
      throw new ConnectionNotFoundException(
          String.format("Connection with ID=%s is not a CCloud connection.", connectionId));
    }
  }
}
