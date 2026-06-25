package io.confluent.idesidecar.restapi.util;

import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.client.HttpResponse;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Adaptive, non-blocking rate limiter for outbound CCloud API requests, keyed per endpoint.
 *
 * <p>CCloud enforces rate limits per endpoint, not as one shared budget; observed
 * {@code X-RateLimit-Limit} values vary (5, 20, 40, 300) across endpoints in the same session.
 * This class therefore holds a {@link Bucket} per endpoint key (URL path), and each bucket
 * independently observes its own {@code X-RateLimit-*} headers.
 *
 * <p>Before a bucket's first server response, it falls back to a configurable default rate.
 * Once headers are observed, the bucket paces requests proportionally across the remaining
 * window.
 *
 * <p>Thread safety: the bucket registry is a {@link ConcurrentHashMap}; each bucket uses atomic
 * state with CAS loops; returned {@link Uni}s use non-blocking delays so the Vert.x event loop
 * stays free.
 */
@ApplicationScoped
@RegisterForReflection
public class CCloudApiRateLimiter {

  private final double defaultPermitsPerSecond;
  private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

  /**
   * @param defaultPermitsPerSecond fallback rate before the first server response with
   *     {@code X-RateLimit-*} headers arrives for a given bucket. Kept as a {@code double} so
   *     very-slow APIs can be configured at fractional rates (e.g., 0.5 req/sec); must be positive.
   */
  public CCloudApiRateLimiter(
      @ConfigProperty(
          name = "ide-sidecar.connections.ccloud.rate-limit.default-permits-per-second",
          defaultValue = "4.0"
      )
      double defaultPermitsPerSecond
  ) {
    if (defaultPermitsPerSecond <= 0) {
      throw new IllegalArgumentException(
          "default-permits-per-second must be positive, got: " + defaultPermitsPerSecond
      );
    }
    this.defaultPermitsPerSecond = defaultPermitsPerSecond;
  }

  /**
   * Acquire a single rate-limit permit from the bucket for {@code url}. Returns immediately if
   * throughput is safe, or returns a {@link Uni} that completes after the necessary delay.
   * Callers must follow each {@link #acquire} with exactly one of {@link #recordResponse},
   * {@link #recordRateLimitedResponse}, or {@link #notifyRequestCompleted} for the same URL.
   */
  public Uni<Void> acquire(String url) {
    return bucketFor(url).acquire();
  }

  /**
   * Record a successful HTTP response: parse {@code X-RateLimit-*} headers if present and feed
   * them to the matching bucket; otherwise just release the inflight permit.
   */
  public void recordResponse(String url, HttpResponse<?> response) {
    bucketFor(url).recordResponse(response);
  }

  /**
   * Record a 429 response: parse {@code Retry-After} (falling back to a 1-second pause if absent
   * or unparseable) and feed an exhaustion state to the matching bucket.
   */
  public void recordRateLimitedResponse(String url, HttpResponse<?> response) {
    bucketFor(url).recordRateLimitedResponse(response);
  }

  /**
   * Release the inflight permit for {@code url}'s bucket without updating its state. Use when a
   * request fails before any response headers are available (network error, etc.).
   */
  public void notifyRequestCompleted(String url) {
    bucketFor(url).notifyRequestCompleted();
  }

  /** Returns the latest observed state for {@code url}'s bucket, or null if none yet. */
  public RateLimitState getLatestState(String url) {
    var bucket = buckets.get(bucketKey(url));
    return bucket == null ? null : bucket.latestState.get();
  }

  /** Number of distinct endpoint buckets observed; useful for diagnostics and tests. */
  public int bucketCount() {
    return buckets.size();
  }

  /**
   * Clear all buckets. Intended for tests that share the {@code @ApplicationScoped} singleton
   * across methods, where leftover state would pollute later assertions.
   */
  public void reset() {
    buckets.clear();
  }

  /**
   * Derive a bucket key from a request URL. We use the URL path (no host, no query string),
   * since CCloud's budgets are per-API not per-resource (e.g., {@code /api/cmk/v2/clusters} is
   * one bucket regardless of {@code environment=}).
   *
   * <p>A leading {@code /api/} prefix is stripped so first-page calls (which include the prefix)
   * and pagination next-page calls (whose {@code metadata.next} URL drops it) share one bucket.
   */
  static String bucketKey(String url) {
    if (url == null || url.isEmpty()) {
      return "";
    }
    try {
      var path = URI.create(url).getPath();
      if (path == null || path.isEmpty()) {
        return url;
      }
      return path.startsWith("/api/") ? path.substring("/api".length()) : path;
    } catch (IllegalArgumentException e) {
      Log.warnf(
          "Could not parse URL for rate-limit bucket key, using raw URL: %s (%s)",
          url, e.getMessage()
      );
      return url;
    }
  }

  private Bucket bucketFor(String url) {
    return buckets.computeIfAbsent(
        bucketKey(url),
        k -> new Bucket(k, defaultPermitsPerSecond)
    );
  }

  /**
   * Per-endpoint state and pacing logic. One instance per bucket key; the outer limiter is a
   * registry/dispatcher over these.
   */
  private static final class Bucket {

    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    // Heuristic for distinguishing relative-seconds vs absolute-epoch-seconds Reset values.
    // CCloud's GraphQL endpoints emit small relative values (<= 60s typically); auth's
    // /oauth/token has been observed emitting absolute Unix epoch timestamps (~1.78 billion).
    // Anything larger than a year of seconds is almost certainly an epoch.
    private static final double EPOCH_RESET_THRESHOLD_SECONDS = 31_536_000.0;

    private final String key;
    private final double defaultPermitsPerSecond;
    private final AtomicReference<RateLimitState> latestState = new AtomicReference<>(null);
    private final AtomicInteger inflight = new AtomicInteger(0);
    private final AtomicLong lastAdmitNanos = new AtomicLong(0);
    private final AtomicBoolean wasExhausted = new AtomicBoolean(false);

    Bucket(String key, double defaultPermitsPerSecond) {
      this.key = key;
      this.defaultPermitsPerSecond = defaultPermitsPerSecond;
    }

    Uni<Void> acquire() {
      long now = System.nanoTime();
      long minIntervalNanos = computeMinIntervalNanos(now);

      while (true) {
        long lastAdmit = lastAdmitNanos.get();
        long target = Math.max(now, lastAdmit + minIntervalNanos);
        if (lastAdmitNanos.compareAndSet(lastAdmit, target)) {
          inflight.incrementAndGet();
          long waitNanos = target - now;
          if (waitNanos <= 0) {
            return Uni.createFrom().voidItem();
          }
          long waitMs = Math.max(1, waitNanos / 1_000_000);
          Log.debugf(
              "Rate limiter %s: waiting %dms for permit (inflight=%d)",
              key, (Object) waitMs, (Object) inflight.get()
          );
          return Uni.createFrom().voidItem()
              .onItem().delayIt().by(Duration.ofMillis(waitMs));
        }
      }
    }

    void recordResponse(HttpResponse<?> response) {
      var state = RateLimitState.fromHeaders(response);
      if (state != null) {
        update(normalizeReset(state));
      } else {
        notifyRequestCompleted();
      }
    }

    void recordRateLimitedResponse(HttpResponse<?> response) {
      int retryAfter = RateLimitState.parseRetryAfterHeader(response);
      update(new RateLimitState(
          -1, 0,
          retryAfter > 0 ? retryAfter : 1.0,
          System.nanoTime()
      ));
    }

    void notifyRequestCompleted() {
      inflight.decrementAndGet();
    }

    private void update(RateLimitState state) {
      inflight.decrementAndGet();
      latestState.getAndUpdate(current ->
          current == null || state.receivedAtNanos() >= current.receivedAtNanos()
              ? state
              : current
      );
      if (state.remaining() > 0 && wasExhausted.compareAndSet(true, false)) {
        Log.infof(
            "Bucket %s: rate-limit budget recovered: remaining=%d, reset=%.1fs",
            key, (Object) state.remaining(), state.resetSeconds()
        );
      }
      Log.debugf(
          "Bucket %s: updated remaining=%d, reset=%.1fs, limit=%d",
          key, (Object) state.remaining(), state.resetSeconds(), (Object) state.limit()
      );
    }

    /**
     * Convert an absolute Unix-epoch Reset value into a relative seconds-until-reset; pass
     * relative values through unchanged. Defends against {@code /oauth/token}'s epoch semantics
     * (every other observed endpoint uses relative seconds).
     */
    private RateLimitState normalizeReset(RateLimitState raw) {
      if (raw.resetSeconds() <= EPOCH_RESET_THRESHOLD_SECONDS) {
        return raw;
      }
      double nowEpoch = System.currentTimeMillis() / 1000.0;
      double relative = Math.max(0.0, raw.resetSeconds() - nowEpoch);
      Log.warnf(
          "Bucket %s: X-RateLimit-Reset=%.0f looks like an absolute epoch timestamp;"
              + " interpreting as %.1fs from now",
          key, raw.resetSeconds(), relative
      );
      return new RateLimitState(raw.limit(), raw.remaining(), relative, raw.receivedAtNanos());
    }

    private long computeMinIntervalNanos(long now) {
      var state = latestState.get();
      if (state == null) {
        return (long) (NANOS_PER_SECOND / defaultPermitsPerSecond);
      }

      double elapsedSinceState = (now - state.receivedAtNanos()) / NANOS_PER_SECOND;
      double remainingWindow = Math.max(0.1, state.resetSeconds() - elapsedSinceState);

      // Only the server's reported budget can declare actual exhaustion. A 429 is the explicit
      // signal: recordRateLimitedResponse stores remaining=0, which lands us here. The local
      // inflight counter must NOT trigger a full-window pause - during the initial fan-out,
      // inflight briefly exceeds remaining (we admit several requests before any response feeds
      // updated state back), and treating that as exhaustion produces spurious 1-second pauses.
      if (state.remaining() <= 0) {
        if (wasExhausted.compareAndSet(false, true)) {
          Log.infof(
              "Bucket %s: rate-limit budget exhausted, pausing %.1fs until window resets",
              key, remainingWindow
          );
        }
        return (long) (remainingWindow * NANOS_PER_SECOND);
      }

      // Pace remaining requests evenly across the window, subtracting inflight so concurrent
      // in-flight calls tighten the spacing but never force a full pause. Clamp to >=1 so a
      // momentary inflight overshoot still produces a sensible interval (window/2 worst case).
      int effectiveRemaining = Math.max(1, state.remaining() - inflight.get());
      return (long) ((remainingWindow / (effectiveRemaining + 1)) * NANOS_PER_SECOND);
    }
  }
}
