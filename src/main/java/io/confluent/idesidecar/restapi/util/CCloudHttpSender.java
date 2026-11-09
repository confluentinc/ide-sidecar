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
 * <p>Each attempt runs under its own rate-limiter permit ({@link CCloudApiRateLimiter#withPermit}),
 * and the sender is invoked fresh on each attempt so a retry re-sends rather than replaying a
 * completed {@code CompletionStage}. A 429 records the bucket's exhausted state and throws
 * {@link TooManyRequestsException} so the retry policy can wait out {@code Retry-After}; any
 * other response records its {@code X-RateLimit-*} headers.
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
    retryPolicy.applyRetryWithoutCutoff(url,
        rateLimiter.withPermit(
            url,
            // record the response inside the attempt, so the bucket's state is updated before the
            // permit is released and the next request reads it
            () -> Uni.createFrom()
                .completionStage(sender.get().toCompletionStage())
                .invoke(response -> recordResponse(url, response))
        )
    ).subscribe().with(promise::complete, promise::fail);
    return promise.future();
  }

  /**
   * Feed the response to the rate limiter, and throw {@link TooManyRequestsException} on a 429 so
   * the retry policy can wait out {@code Retry-After}.
   */
  private void recordResponse(String url, HttpResponse<Buffer> response) {
    if (response.statusCode() == 429) {
      rateLimiter.recordRateLimitedResponse(url, response);
      int retryAfter = RateLimitState.parseRetryAfterHeader(response);
      Log.warnf(
          "Rate limited by %s (retry-after: %ds, X-RateLimit-Limit=%s, Remaining=%s, Reset=%s)",
          url, retryAfter,
          response.getHeader(RateLimitState.HEADER_LIMIT),
          response.getHeader(RateLimitState.HEADER_REMAINING),
          response.getHeader(RateLimitState.HEADER_RESET));
      throw new TooManyRequestsException(url, retryAfter);
    }
    rateLimiter.recordResponse(url, response);
  }
}
