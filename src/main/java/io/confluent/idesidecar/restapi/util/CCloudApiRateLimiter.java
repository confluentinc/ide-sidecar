package io.confluent.idesidecar.restapi.util;

import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Non-blocking token-bucket rate limiter for outbound CCloud API requests. CCloud enforces a hard
 * limit of 5 requests per second; this limiter is configured below that to leave headroom for
 * auth-related calls that flow through a separate code path.
 *
 * <p>The limiter uses a simple token bucket: tokens refill at a constant rate, and each
 * {@link #acquire()} call consumes one token. If no token is available, the returned {@link Uni}
 * is delayed until a token becomes available, keeping the Vert.x event loop unblocked.
 */
@ApplicationScoped
@RegisterForReflection
public class CCloudApiRateLimiter {

  private final double permitsPerSecond;
  private final AtomicReference<TokenBucketState> state;

  /**
   * Internal state of the token bucket.
   *
   * @param availablePermits current number of available permits (fractional during refill)
   * @param lastRefillNanos  timestamp (System.nanoTime) of the last refill calculation
   */
  private record TokenBucketState(double availablePermits, long lastRefillNanos) {
  }

  public CCloudApiRateLimiter(
      @ConfigProperty(
          name = "ide-sidecar.connections.ccloud.rate-limit.permits-per-second",
          defaultValue = "4.0"
      )
      double permitsPerSecond
  ) {
    this.permitsPerSecond = permitsPerSecond;
    this.state = new AtomicReference<>(
        new TokenBucketState(permitsPerSecond, System.nanoTime())
    );
  }

  /**
   * Acquire a single rate-limit permit. Returns immediately if a permit is available, or returns
   * a {@link Uni} that completes after the necessary delay.
   */
  public Uni<Void> acquire() {
    while (true) {
      var current = state.get();
      long now = System.nanoTime();
      double elapsedSeconds = (now - current.lastRefillNanos) / 1_000_000_000.0;

      // refill tokens based on elapsed time, capped at the bucket size (1 second of burst)
      double refilled = Math.min(
          permitsPerSecond,
          current.availablePermits + elapsedSeconds * permitsPerSecond
      );

      if (refilled >= 1.0) {
        // a permit is available: consume it
        var next = new TokenBucketState(refilled - 1.0, now);
        if (state.compareAndSet(current, next)) {
          return Uni.createFrom().voidItem();
        }
        // CAS failed, retry the loop
      } else {
        // no permit available: calculate wait time
        double deficit = 1.0 - refilled;
        long waitMs = Math.max(1, (long) Math.ceil(deficit / permitsPerSecond * 1000));
        // update the refill timestamp so concurrent callers spread out
        var next = new TokenBucketState(refilled, now);
        if (state.compareAndSet(current, next)) {
          Log.debugf("Rate limiter: waiting %dms for CCloud API permit", waitMs);
          return Uni.createFrom().voidItem()
              .onItem().delayIt().by(Duration.ofMillis(waitMs))
              .chain(() -> acquire());
        }
        // CAS failed, retry the loop
      }
    }
  }

  /**
   * Returns the configured permits-per-second rate, for testing.
   */
  double getPermitsPerSecond() {
    return permitsPerSecond;
  }
}
