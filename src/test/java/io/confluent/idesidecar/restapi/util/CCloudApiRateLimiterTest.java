package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class CCloudApiRateLimiterTest {

  @Test
  void shouldThrottleWhenBurstExceeded() {
    // arrange: 2 permits/sec means the bucket starts with 2 permits.
    // acquiring 4 permits should require waiting for 2 more to refill.
    var limiter = new CCloudApiRateLimiter(2.0);

    // act: drain the initial burst (2 permits) then acquire 2 more
    var start = Instant.now();
    for (int i = 0; i < 4; i++) {
      limiter.acquire().await().atMost(Duration.ofSeconds(5));
    }
    var elapsed = Duration.between(start, Instant.now());

    // assert: the extra 2 permits should take ~1s to refill at 2/sec
    assertTrue(
        elapsed.toMillis() >= 800,
        "Expected >= 800ms for 4 permits at 2/sec (2 burst + 2 wait), got %dms".formatted(
            elapsed.toMillis()
        )
    );
  }

  @Test
  void shouldReturnImmediatelyWhenPermitAvailable() {
    // arrange: high rate so permits are always available
    var limiter = new CCloudApiRateLimiter(1000.0);

    // act
    var start = Instant.now();
    limiter.acquire().await().atMost(Duration.ofSeconds(1));
    var elapsed = Duration.between(start, Instant.now());

    // assert: should be nearly instant
    assertTrue(
        elapsed.toMillis() < 100,
        "Expected immediate permit, but took %dms".formatted(elapsed.toMillis())
    );
  }

  @Test
  void shouldAllowBurstUpToRate() {
    // arrange: 5 permits/sec starts with 5 permits in the bucket
    var limiter = new CCloudApiRateLimiter(5.0);

    // act: acquire 5 permits (the full initial burst)
    var timestamps = new ArrayList<Instant>();
    for (int i = 0; i < 5; i++) {
      limiter.acquire().await().atMost(Duration.ofSeconds(2));
      timestamps.add(Instant.now());
    }

    // assert: all 5 should complete within the burst window (very fast)
    var elapsed = Duration.between(timestamps.getFirst(), timestamps.getLast());
    assertTrue(
        elapsed.toMillis() < 200,
        "Expected burst of 5 permits to be fast, but took %dms".formatted(elapsed.toMillis())
    );
  }

  @Test
  void shouldReportConfiguredRate() {
    var limiter = new CCloudApiRateLimiter(4.0);
    assertEquals(4.0, limiter.getPermitsPerSecond());
  }
}
