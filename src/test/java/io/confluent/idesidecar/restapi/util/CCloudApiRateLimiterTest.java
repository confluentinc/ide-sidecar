package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
      limiter.acquire().await().atMost(Duration.ofSeconds(10));
    }
    var elapsed = Duration.between(start, Instant.now());

    // assert: the extra 2 permits at 2/sec means ~1s of waiting.
    // wide threshold (500ms) to tolerate CI load and JVM warmup.
    assertTrue(
        elapsed.toMillis() >= 500,
        "Expected >= 500ms for 4 permits at 2/sec (2 burst + 2 wait), got %dms".formatted(
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
    limiter.acquire().await().atMost(Duration.ofSeconds(2));
    var elapsed = Duration.between(start, Instant.now());

    // assert: no intentional delay should be applied.
    // generous threshold to tolerate slow CI runners and JVM warmup.
    assertTrue(
        elapsed.toMillis() < 500,
        "Expected no intentional delay, but took %dms".formatted(elapsed.toMillis())
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

    // assert: all 5 should complete within the burst window (no intentional delay)
    var elapsed = Duration.between(timestamps.getFirst(), timestamps.getLast());
    assertTrue(
        elapsed.toMillis() < 500,
        "Expected burst of 5 permits to be fast, but took %dms".formatted(elapsed.toMillis())
    );
  }

  @Test
  void shouldReportConfiguredRate() {
    var limiter = new CCloudApiRateLimiter(4.0);
    assertEquals(4.0, limiter.getPermitsPerSecond());
  }

  @Test
  void shouldRejectNonPositiveRate() {
    assertThrows(IllegalArgumentException.class, () -> new CCloudApiRateLimiter(0.0));
    assertThrows(IllegalArgumentException.class, () -> new CCloudApiRateLimiter(-1.0));
  }
}
