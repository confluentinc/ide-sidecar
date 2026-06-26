package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.TooManyRequestsException;
import io.confluent.idesidecar.restapi.util.CCloudApiRateLimiter;
import io.confluent.idesidecar.restapi.util.RateLimitState;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.client.HttpResponse;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Base REST client for CCloud API calls. Adds CCloud-specific auth headers and the
 * adaptive rate-limit + 429 retry behavior. The rest of the request pipeline lives on
 * {@link ConfluentRestClient}; non-CCloud subclasses inherit none of the machinery here.
 */
@RegisterForReflection
public abstract class ConfluentCloudRestClient extends ConfluentRestClient {

  @Inject
  CCloudApiRateLimiter rateLimiter;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.initial-backoff-ms",
      defaultValue = "500"
  )
  long retryInitialBackoffMs;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.max-backoff-ms",
      defaultValue = "10000"
  )
  long retryMaxBackoffMs;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.max-retries",
      defaultValue = "5"
  )
  int retryMaxRetries;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.jitter-factor",
      defaultValue = "0.2"
  )
  double retryJitterFactor;

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

  /**
   * Wrap each outbound request with CCloud's adaptive rate-limit permit acquisition and 429
   * retry. The supplier produces the inner uni (send + checkResponse + parse + onResponseReceived)
   * fresh on each subscribe so retries get a new send rather than a replayed CompletionStage.
   * Adds the acquire-before / release-on-failure semantics plus a retry-on-429 chain with
   * exponential backoff + jitter; logs at ERROR on retry exhaustion so operators can distinguish
   * exhausted retries from a single unretried 429.
   */
  @Override
  protected <T> Uni<T> wrapRequest(String url, Supplier<Uni<T>> uniSupplier) {
    return rateLimiter.acquire(url)
        .chain(uniSupplier::get)
        .onFailure(t -> !(t instanceof TooManyRequestsException))
        .invoke(t -> rateLimiter.notifyRequestCompleted(url))
        .onFailure(TooManyRequestsException.class)
        .retry()
        .withBackOff(
            Duration.ofMillis(retryInitialBackoffMs),
            Duration.ofMillis(retryMaxBackoffMs)
        )
        .withJitter(retryJitterFactor)
        .atMost(retryMaxRetries)
        .onFailure(TooManyRequestsException.class)
        .invoke(t -> Log.errorf(
            "Rate-limit retries exhausted for %s after %d retries; propagating failure",
            url, retryMaxRetries
        ));
  }

  /**
   * Feed the {@code X-RateLimit-*} response headers into the limiter so subsequent acquires can
   * pace against the observed window. Called after a successful parse (see {@code listItems}
   * ordering); this also releases the inflight permit.
   */
  @Override
  protected void onResponseReceived(String url, HttpResponse<?> response) {
    rateLimiter.recordResponse(url, response);
  }

  /**
   * Handle 429 responses before delegating non-429 errors to the base. On 429 we record the
   * exhausted state (decrements inflight, stores reset-window info), log with full header detail,
   * and throw {@link TooManyRequestsException} so {@link #wrapRequest}'s retry chain fires.
   */
  @Override
  protected void checkResponse(HttpResponse<?> response, String url) {
    if (response.statusCode() == 429) {
      rateLimiter.recordRateLimitedResponse(url, response);
      int retryAfter = RateLimitState.parseRetryAfterHeader(response);
      Log.warnf(
          "Rate limited by %s (retry-after: %ds, X-RateLimit-Limit=%s, Remaining=%s, Reset=%s)",
          url, retryAfter,
          response.getHeader(RateLimitState.HEADER_LIMIT),
          response.getHeader(RateLimitState.HEADER_REMAINING),
          response.getHeader(RateLimitState.HEADER_RESET)
      );
      throw new TooManyRequestsException(url, retryAfter);
    }
    super.checkResponse(response, url);
  }
}
