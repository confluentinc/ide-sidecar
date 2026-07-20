package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.exceptions.TooManyRequestsException;
import io.quarkus.arc.Unremovable;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.Supplier;

/**
 * Sends Future-based CCloud HTTP requests through the shared {@link CCloudApiRateLimiter} and
 * {@link CCloudHttpRetryPolicy}.
 *
 * <p>The sender is invoked fresh on each subscribe so retry-on-429 actually re-sends rather than
 * replaying a completed {@code CompletionStage}. The limiter sees:
 * <ul>
 *   <li>{@code acquire} on subscribe (inflight++)</li>
 *   <li>{@code recordRateLimitedResponse} on 429 (decrements inflight, stores exhausted state);
 *       a {@link TooManyRequestsException} is then thrown so the retry chain fires</li>
 *   <li>{@code recordResponse} on every non-429 response (decrements inflight, records
 *       {@code X-RateLimit-*} state if headers present)</li>
 *   <li>{@code notifyRequestCompleted} on pre-response failure (DNS/TLS/etc.)</li>
 * </ul>
 */
@ApplicationScoped
@Unremovable
@RegisterForReflection
public class CCloudHttpSender {

  @Inject
  CCloudApiRateLimiter rateLimiter;

  @Inject
  CCloudHttpRetryPolicy retryPolicy;

  /**
   * Send a request through the rate limiter and retry policy, bridging Future through Mutiny
   * and back so the outbound API stays Future-shaped for callers.
   */
  public Future<HttpResponse<Buffer>> send(
      String url,
      Supplier<Future<HttpResponse<Buffer>>> sender) {
    Promise<HttpResponse<Buffer>> promise = Promise.promise();
    retryPolicy.applyRetry(url,
        rateLimiter.acquire(url)
            .chain(() -> Uni.createFrom().completionStage(sender.get().toCompletionStage()))
            .invoke(response -> {
              if (response.statusCode() == 429) {
                rateLimiter.recordRateLimitedResponse(url, response);
                int retryAfter = RateLimitState.parseRetryAfterHeader(response);
                Log.warnf(
                    "Rate limited by %s (retry-after: %ds, X-RateLimit-Limit=%s, Remaining=%s, "
                        + "Reset=%s)",
                    url, retryAfter,
                    response.getHeader(RateLimitState.HEADER_LIMIT),
                    response.getHeader(RateLimitState.HEADER_REMAINING),
                    response.getHeader(RateLimitState.HEADER_RESET));
                throw new TooManyRequestsException(url, retryAfter);
              }
              rateLimiter.recordResponse(url, response);
            })
            .onFailure(t -> !(t instanceof TooManyRequestsException))
            .invoke(t -> rateLimiter.notifyRequestCompleted(url))
    ).subscribe().with(promise::complete, promise::fail);
    return promise.future();
  }
}
