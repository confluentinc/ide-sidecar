package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.util.CCloudApiRateLimiter;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;
import jakarta.inject.Inject;

/**
 * Base REST client for CCloud API calls. Adds CCloud-specific auth headers and routes outbound
 * requests through the {@link CCloudApiRateLimiter} to stay within CCloud's rate limits.
 */
@RegisterForReflection
public abstract class ConfluentCloudRestClient extends ConfluentRestClient {

  @Inject
  CCloudApiRateLimiter rateLimiter;

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

  @Override
  protected Uni<Void> acquireRateLimitPermit() {
    return rateLimiter.acquire();
  }
}
