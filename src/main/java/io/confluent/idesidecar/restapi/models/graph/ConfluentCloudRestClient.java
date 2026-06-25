package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.util.CCloudApiRateLimiter;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.client.HttpResponse;
import jakarta.inject.Inject;

/**
 * Base REST client for CCloud API calls. Adds CCloud-specific auth headers and routes outbound
 * requests through the {@link CCloudApiRateLimiter}, which dynamically adjusts throughput based
 * on rate-limit response headers from the CCloud API.
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
  protected Uni<Void> acquireRateLimitPermit(String url) {
    return rateLimiter.acquire(url);
  }

  @Override
  protected void onResponseReceived(String url, HttpResponse<?> response) {
    rateLimiter.recordResponse(url, response);
  }

  @Override
  protected void onRateLimitResponse(String url, HttpResponse<?> response) {
    rateLimiter.recordRateLimitedResponse(url, response);
  }

  @Override
  protected void onRequestFailed(String url) {
    rateLimiter.notifyRequestCompleted(url);
  }
}
