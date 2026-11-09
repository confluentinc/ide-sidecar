package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.exceptions.TooManyRequestsException;
import io.confluent.idesidecar.restapi.exceptions.TransientServerErrorException;
import io.netty.channel.ConnectTimeoutException;
import io.smallrye.mutiny.Uni;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLHandshakeException;
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

    assertThrows(
        TooManyRequestsException.class,
        () -> policy.applyRetry(URL, startedLongAgo, source).await().atMost(Duration.ofSeconds(5))
    );

    assertEquals(1, attempts.get());
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

  @Test
  void idempotentRetryShouldRetryUnavailableServersAndConnectFailures() {
    assertTrue(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(
        new TransientServerErrorException(URL, 503, -1, new RuntimeException("503"))
    ));
    assertTrue(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(
        new CompletionException(new ConnectException("refused"))
    ));
    assertTrue(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(
        new UnknownHostException("api.confluent.cloud")
    ));
    assertTrue(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(
        new TooManyRequestsException(URL, 1)
    ));
  }

  @Test
  void idempotentRetryShouldOnlyRetry429sAnd5xxThrownDirectly() {
    // the Retry-After lookup and the unwrap on exhaustion read only the top-level exception
    assertFalse(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(new CompletionException(
        new TransientServerErrorException(URL, 503, 30, new RuntimeException("503"))
    )));
    assertFalse(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(
        new CompletionException(new TooManyRequestsException(URL, 30))
    ));
  }

  @Test
  void idempotentRetryShouldNotRetryTimeoutsOrTlsFailures() {
    assertFalse(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(
        new ConnectTimeoutException("connect timed out")
    ));
    assertFalse(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(
        new TimeoutException("no data for 20s")
    ));
    assertFalse(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(
        new SSLHandshakeException("bad certificate")
    ));
    assertFalse(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(
        new IllegalStateException("not a network failure")
    ));
    var refusedDuringTls = new ConnectException("refused");
    refusedDuringTls.initCause(new SSLHandshakeException("bad certificate"));
    assertFalse(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(refusedDuringTls));
    var dnsTimeout = new UnknownHostException("api.confluent.cloud");
    dnsTimeout.initCause(new TimeoutException("DNS query timed out"));
    assertFalse(CCloudHttpRetryPolicy.isRetryableForIdempotentRequest(dnsTimeout));
  }

  @Test
  void applyRetryShouldNotRetryConnectFailuresForNonIdempotentRequests() {
    shortenWaits();
    var attempts = new AtomicInteger();
    var source = Uni.createFrom().<String>deferred(() -> {
      attempts.incrementAndGet();
      return Uni.createFrom().failure(new ConnectException("refused"));
    });

    var thrown = assertThrows(
        CompletionException.class,
        () -> policy.applyRetry(URL, source).await().atMost(Duration.ofSeconds(5))
    );

    assertInstanceOf(ConnectException.class, thrown.getCause());

    assertEquals(1, attempts.get());
  }

  @Test
  void idempotentRetryShouldPropagateTheLastFailureWhenRetriesRunOut() {
    shortenWaits();
    var attempts = new AtomicInteger();
    var source = Uni.createFrom().<String>deferred(() -> Uni.createFrom().failure(
        new ConnectException("refused " + attempts.incrementAndGet())
    ));

    var thrown = assertThrows(
        CompletionException.class,
        () -> policy.applyIdempotentRetry(URL, null, source).await().atMost(Duration.ofSeconds(5))
    );

    assertEquals(3, attempts.get());
    assertEquals("refused 3", thrown.getCause().getMessage());
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
