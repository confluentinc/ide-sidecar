package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.exceptions.TooManyRequestsException;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CCloudHttpRetryPolicyTest {

  private static final String URL = "https://api.confluent.cloud/org/v2/environments";
  private static final Instant NOW = Instant.parse("2026-11-06T12:00:00Z");
  private static final Instant FAR_DEADLINE = NOW.plusSeconds(600);

  private final CCloudHttpRetryPolicy policy = new CCloudHttpRetryPolicy();

  @BeforeEach
  void configure() {
    policy.initialBackoffMs = 1_000;
    policy.maxBackoffMs = 10_000;
    policy.maxRetries = 5;
    policy.jitterFactor = 0.2;
    policy.maxTotalMs = 40_000;
  }

  @Test
  void shouldWaitAtLeastRetryAfter() {
    var wait = policy.nextDelay(0, 30, NOW, FAR_DEADLINE, () -> 0.0);

    assertEquals(Duration.ofSeconds(30), wait.orElseThrow());
  }

  @Test
  void shouldAddJitterOnTopOfRetryAfter() {
    var wait = policy.nextDelay(0, 30, NOW, FAR_DEADLINE, () -> 1.0);

    assertEquals(Duration.ofSeconds(36), wait.orElseThrow());
  }

  @Test
  void shouldUseBackoffWhenRetryAfterIsZero() {
    var wait = policy.nextDelay(0, 0, NOW, FAR_DEADLINE, () -> 0.0);

    assertEquals(Duration.ofSeconds(1), wait.orElseThrow());
  }

  @Test
  void shouldDoubleTheBackoffUpToTheMax() {
    assertEquals(Duration.ofSeconds(2), backoffWithoutRetryAfter(1));
    assertEquals(Duration.ofSeconds(8), backoffWithoutRetryAfter(3));
    assertEquals(Duration.ofSeconds(10), backoffWithoutRetryAfter(4));
  }

  @Test
  void shouldNotRetryWhenRetryAfterWouldPassTheDeadline() {
    var wait = policy.nextDelay(0, 30, NOW, NOW.plusSeconds(10), () -> 0.0);

    assertTrue(wait.isEmpty());
  }

  @Test
  void shouldNotRetryAfterTheLastAttempt() {
    var wait = policy.nextDelay(5, 1, NOW, FAR_DEADLINE, () -> 0.0);

    assertTrue(wait.isEmpty());
  }

  @Test
  void applyRetryShouldPropagateTheLast429WhenRetriesRunOut() {
    shortenWaits();
    var attempts = new AtomicInteger();
    var source = Uni.createFrom().<String>deferred(() -> Uni.createFrom().failure(
        new TooManyRequestsException(URL, attempts.incrementAndGet())
    ));

    var thrown = assertThrows(
        TooManyRequestsException.class,
        () -> policy.applyRetry(URL, source).await().atMost(Duration.ofSeconds(5))
    );

    // 1 try and 2 retries, and the failure is the last 429, Retry-After included
    assertEquals(3, attempts.get());
    assertEquals(3, thrown.getRetryAfterSeconds());
  }

  @Test
  void applyRetryShouldStopWhenTheOperationsBudgetIsSpent() {
    shortenWaits();
    var attempts = new AtomicInteger();
    var source = Uni.createFrom().<String>deferred(() -> {
      attempts.incrementAndGet();
      return Uni.createFrom().failure(new TooManyRequestsException(URL, 0));
    });
    var startedLongAgo = Instant.now().minusMillis(policy.maxTotalMs);

    var thrown = assertThrows(
        CompletionException.class,
        () -> policy.applyRetry(URL, startedLongAgo, source).await().atMost(Duration.ofSeconds(5))
    );

    assertInstanceOf(TimeoutException.class, thrown.getCause());
    assertEquals("Gave up on " + URL + " after 5s", thrown.getCause().getMessage());
    assertEquals(0, attempts.get());
  }

  @Test
  void applyRetryShouldGiveUpOnARequestThatOutlastsTheBudget() {
    policy.maxTotalMs = 200;
    var started = System.nanoTime();

    var thrown = assertThrows(
        CompletionException.class,
        () -> policy.applyRetry(URL, Uni.createFrom().nothing())
            .await().atMost(Duration.ofSeconds(5))
    );

    var elapsed = Duration.ofNanos(System.nanoTime() - started);
    assertInstanceOf(TimeoutException.class, thrown.getCause());
    assertTrue(elapsed.toMillis() >= 200, "gave up before the budget: " + elapsed);
    assertTrue(elapsed.toMillis() < 2_000, "took " + elapsed);
  }

  @Test
  void applyRetryShouldNotRetryOtherFailures() {
    shortenWaits();
    var attempts = new AtomicInteger();
    var source = Uni.createFrom().<String>deferred(() -> {
      attempts.incrementAndGet();
      return Uni.createFrom().failure(new IllegalStateException("not a 429"));
    });

    assertThrows(
        IllegalStateException.class,
        () -> policy.applyRetry(URL, source).await().atMost(Duration.ofSeconds(5))
    );

    assertEquals(1, attempts.get());
  }

  private Duration backoffWithoutRetryAfter(int retries) {
    return policy.nextDelay(retries, -1, NOW, FAR_DEADLINE, () -> 0.0).orElseThrow();
  }

  private void shortenWaits() {
    policy.initialBackoffMs = 1;
    policy.maxBackoffMs = 1;
    policy.maxRetries = 2;
    policy.maxTotalMs = 5_000;
  }
}
