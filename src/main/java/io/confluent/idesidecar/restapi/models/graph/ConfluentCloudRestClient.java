package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.TooManyRequestsException;
import io.confluent.idesidecar.restapi.exceptions.TransientServerErrorException;
import io.confluent.idesidecar.restapi.util.CCloudApiRateLimiter;
import io.confluent.idesidecar.restapi.util.CCloudHttpRetryPolicy;
import io.confluent.idesidecar.restapi.util.RateLimitState;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.client.HttpResponse;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Base REST client for CCloud API calls. Adds CCloud-specific auth headers and the
 * adaptive rate-limit + 429 retry behavior. The rest of the request pipeline lives on
 * {@link ConfluentRestClient}; non-CCloud subclasses inherit none of the machinery here.
 */
@RegisterForReflection
public abstract class ConfluentCloudRestClient extends ConfluentRestClient {

  @Inject
  CCloudApiRateLimiter rateLimiter;

  @Inject
  CCloudHttpRetryPolicy retryPolicy;

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
   * Wrap each outbound request with CCloud's adaptive rate limiting and retries. The supplier
   * produces the inner uni (send + checkResponse + parse + onResponseReceived) fresh on each
   * subscribe so retries get a new send rather than a replayed CompletionStage. Each attempt runs
   * under its own rate-limiter permit. 429s, 502/503/504 responses, refused connections and
   * failed DNS lookups are retried per {@link CCloudHttpRetryPolicy#applyIdempotentRetry}, which
   * waits out {@code Retry-After} and logs at ERROR when it gives up. That's only safe because
   * every request here is a GET from {@code listItems} or {@code getItem}.
   */
  @Override
  protected <T> Uni<T> wrapRequest(String url, Supplier<Uni<T>> uniSupplier) {
    return wrapRequest(url, null, uniSupplier);
  }

  /**
   * Like {@link #wrapRequest(String, Supplier)}, with the retry budget starting at
   * {@code startedAt}.
   */
  @Override
  protected <T> Uni<T> wrapRequest(String url, Instant startedAt, Supplier<Uni<T>> uniSupplier) {
    // every fetch here is a GET, so 5xx and connect failures are safe to retry too
    return retryPolicy
        .applyIdempotentRetry(url, startedAt, rateLimiter.withPermit(url, uniSupplier))
        // a 5xx that wasn't retried away fails with the same error it always has
        .onFailure(TransientServerErrorException.class)
        .transform(Throwable::getCause);
  }

  /**
   * Feed the {@code X-RateLimit-*} response headers into the limiter so subsequent acquires can
   * pace against the observed window. Called after a successful parse (see {@code listItems}
   * ordering). The permit itself is released by {@code withPermit} when the attempt ends.
   */
  @Override
  protected void onResponseReceived(String url, HttpResponse<?> response) {
    rateLimiter.recordResponse(url, response);
  }

  /**
   * Handle 429 responses before delegating other errors to the base. On 429 we record the
   * exhausted state (stores reset-window info), log with full header detail, and throw
   * {@link TooManyRequestsException} so {@link #wrapRequest}'s retry chain fires. The base's error
   * for a 502, 503 or 504 is wrapped in a {@link TransientServerErrorException} so it's retried
   * too, and {@link #wrapRequest} unwraps it if the retries run out.
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
    try {
      super.checkResponse(response, url);
    } catch (RuntimeException error) {
      var status = response.statusCode();
      if (status == 502 || status == 503 || status == 504) {
        throw new TransientServerErrorException(
            url, status, RateLimitState.parseRetryAfterHeader(response), error
        );
      }
      throw error;
    }
  }
}
