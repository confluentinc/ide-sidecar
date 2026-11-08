package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.exceptions.TooManyRequestsException;
import io.confluent.idesidecar.restapi.exceptions.TransientServerErrorException;
import io.netty.channel.ConnectTimeoutException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;
import javax.net.ssl.SSLException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Retries outbound CCloud calls that fail with {@link TooManyRequestsException} (HTTP 429), and,
 * for requests that are safe to send twice, 502, 503 and 504 responses and connection failures
 * (see {@link #applyIdempotentRetry}). Each wait is the longer of the server's
 * {@code Retry-After} and an exponential backoff (doubling from {@code initial-backoff-ms} up to
 * {@code max-backoff-ms}), plus up to {@code jitter-factor} more on top. A request stops retrying
 * after {@code max-retries}, or as soon as the next wait would end more than {@code max-total-ms}
 * after the operation started, and then fails with the last error, {@code Retry-After} included.
 * The operation as a whole, including rate-limiter waits and the attempt in flight, also fails
 * with a {@link TimeoutException} once {@code max-total-ms} has passed, except through
 * {@link #applyRetryWithoutCutoff}.
 * Used by both the GraphQL fetcher path ({@code ConfluentCloudRestClient.wrapRequest}) and the
 * auth path ({@code CCloudHttpSender.send}, 429s only).
 *
 * <p>Rate-limiter permits are taken per attempt by
 * {@link CCloudApiRateLimiter#withPermit}, which callers wrap around each attempt.
 */
@ApplicationScoped
@RegisterForReflection
public class CCloudHttpRetryPolicy {

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.initial-backoff-ms",
      defaultValue = "1000"
  )
  long initialBackoffMs;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.max-backoff-ms",
      defaultValue = "10000"
  )
  long maxBackoffMs;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.max-retries",
      defaultValue = "5"
  )
  int maxRetries;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.jitter-factor",
      defaultValue = "0.2"
  )
  double jitterFactor;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.max-total-ms",
      defaultValue = "40000"
  )
  long maxTotalMs;

  /**
   * How long one call, with its retries, may take.
   */
  public Duration budget() {
    return Duration.ofMillis(maxTotalMs);
  }

  /**
   * Retry {@code source} on 429s, with the time budget starting when it's subscribed.
   */
  public <T> Uni<T> applyRetry(String url, Uni<T> source) {
    return applyRetry(url, null, source);
  }

  /**
   * Like {@link #applyRetry(String, Uni)}, for a request whose answer mustn't be dropped once it's
   * sent, such as a token exchange that rotates the refresh token. The budget only stops new
   * retries; an attempt in flight runs to its own request timeout.
   */
  public <T> Uni<T> applyRetryWithoutCutoff(String url, Uni<T> source) {
    // per subscription, so a resubscribed source starts over
    return Uni.createFrom().deferred(
        () -> retryWithin(
            url,
            Instant.now().plusMillis(maxTotalMs),
            source,
            TooManyRequestsException.class::isInstance,
            new Attempts()
        )
    );
  }

  /**
   * Retry {@code source} on 429s, with the time budget starting at {@code startedAt}, such as
   * when the first page of a paged fetch was requested. A null {@code startedAt} starts the
   * budget when {@code source} is subscribed. Fails with a {@link TimeoutException} once the
   * budget has passed. Logs at ERROR when it gives up.
   */
  public <T> Uni<T> applyRetry(String url, Instant startedAt, Uni<T> source) {
    return retry(url, startedAt, source, TooManyRequestsException.class::isInstance);
  }

  /**
   * Like {@link #applyRetry(String, Instant, Uni)}, for a request that's safe to send twice, such
   * as a GET: also retries 502, 503 and 504 responses ({@link TransientServerErrorException}),
   * refused connections, and failed DNS lookups.
   */
  public <T> Uni<T> applyIdempotentRetry(String url, Instant startedAt, Uni<T> source) {
    return retry(url, startedAt, source, CCloudHttpRetryPolicy::isRetryableForIdempotentRequest);
  }

  /**
   * Whether a request that's safe to send twice should be retried after {@code failure}. Timeouts
   * aren't retried, so retries can't outlast the caller's own timeout, and TLS failures aren't
   * either, since a retry won't fix them.
   */
  static boolean isRetryableForIdempotentRequest(Throwable failure) {
    // only when thrown directly, as the Retry-After lookup and the caller's unwrap expect
    if (failure instanceof TooManyRequestsException
        || failure instanceof TransientServerErrorException) {
      return true;
    }
    var retryable = false;
    for (var cause = failure; cause != null; cause = cause.getCause()) {
      if (isTimeoutOrTlsFailure(cause)) {
        // even when wrapped in, or wrapping, a connect or DNS failure
        return false;
      }
      retryable |= cause instanceof ConnectException || cause instanceof UnknownHostException;
    }
    return retryable;
  }

  /**
   * Netty's connect timeout extends {@link ConnectException}, and its DNS timeout isn't a
   * {@link TimeoutException}, hence the name check.
   */
  private static boolean isTimeoutOrTlsFailure(Throwable failure) {
    return failure instanceof TimeoutException
        || failure instanceof SocketTimeoutException
        || failure instanceof ConnectTimeoutException
        || failure instanceof SSLException
        || failure.getClass().getSimpleName().endsWith("TimeoutException");
  }

  private <T> Uni<T> retry(
      String url,
      Instant startedAt,
      Uni<T> source,
      Predicate<Throwable> retryable
  ) {
    // per subscription, so a resubscribed source starts over
    return Uni.createFrom().deferred(() -> {
      // one reading of the clock, so a budget that starts now is exactly maxTotalMs
      var now = Instant.now();
      var deadline = (startedAt != null ? startedAt : now).plusMillis(maxTotalMs);
      var left = Duration.between(now, deadline);
      var attempts = new Attempts();
      if (!left.isPositive()) {
        return Uni.createFrom().failure(budgetSpent(url, attempts));
      }
      return retryWithin(url, deadline, source, retryable, attempts)
          // a hard stop, so nested fetches add up to a known worst case
          .ifNoItem().after(left).failWith(() -> budgetSpent(url, attempts));
    });
  }

  private TimeoutException budgetSpent(String url, Attempts attempts) {
    var retries = attempts.retries.get();
    var lastFailure = attempts.lastFailure.get();
    var message = "Gave up on %s after %s (%d %s%s)".formatted(
        url,
        TimeUtil.humanReadableDuration(Duration.ofMillis(maxTotalMs)),
        retries,
        retries == 1 ? "retry" : "retries",
        lastFailure == null ? "" : "; last failure: " + lastFailure.getMessage()
    );
    Log.error(message);
    return new TimeoutException(message);
  }

  /**
   * The retries one call has made so far, and the last failure it retried.
   */
  private record Attempts(AtomicInteger retries, AtomicReference<Throwable> lastFailure) {

    Attempts() {
      this(new AtomicInteger(), new AtomicReference<>());
    }
  }

  private <T> Uni<T> retryWithin(
      String url,
      Instant deadline,
      Uni<T> source,
      Predicate<Throwable> retryable,
      Attempts attempts
  ) {
    return source
        .onFailure(retryable)
        .retry()
        .when(failures -> {
          var retries = attempts.retries;
          return failures.onItem().transformToUniAndConcatenate(failure -> {
            attempts.lastFailure.set(failure);
            var retry = retries.getAndIncrement();
            var retryAfterSeconds = retryAfterSeconds(failure);
            var wait = nextDelay(
                retry,
                retryAfterSeconds,
                Instant.now(),
                deadline,
                () -> ThreadLocalRandom.current().nextDouble()
            );
            if (wait.isEmpty()) {
              Log.errorf(
                  "Not retrying %s after %d retries (Retry-After: %ds): %s",
                  url,
                  retry,
                  retryAfterSeconds,
                  retry >= maxRetries
                      ? "retries ran out"
                      : "the next wait would pass the %dms budget".formatted(maxTotalMs)
              );
              return Uni.createFrom().failure(failure);
            }
            Log.debugf(
                "Retrying %s in %dms after: %s", url, wait.get().toMillis(), failure.getMessage()
            );
            return Uni.createFrom().item(retry).onItem().delayIt().by(wait.get());
          });
        });
  }

  private static int retryAfterSeconds(Throwable failure) {
    if (failure instanceof TooManyRequestsException tooManyRequests) {
      return tooManyRequests.getRetryAfterSeconds();
    }
    if (failure instanceof TransientServerErrorException unavailable) {
      return unavailable.getRetryAfterSeconds();
    }
    return -1;
  }

  /**
   * How long to wait before retry number {@code retries} (0-based), or empty when it shouldn't
   * happen. A negative {@code retryAfterSeconds} means the 429 had no usable {@code Retry-After}.
   */
  Optional<Duration> nextDelay(
      int retries,
      int retryAfterSeconds,
      Instant now,
      Instant deadline,
      DoubleSupplier random
  ) {
    if (retries >= maxRetries) {
      return Optional.empty();
    }
    var backoff = Duration.ofMillis(
        Math.min(maxBackoffMs, initialBackoffMs << Math.min(retries, 30))
    );
    var retryAfter = Duration.ofSeconds(Math.max(0, retryAfterSeconds));
    var base = retryAfter.compareTo(backoff) > 0 ? retryAfter : backoff;
    var wait = Duration.ofMillis(
        Math.round(base.toMillis() * (1 + jitterFactor * random.getAsDouble()))
    );
    return now.plus(wait).isAfter(deadline) ? Optional.empty() : Optional.of(wait);
  }
}
