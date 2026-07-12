package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class CCloudApiRateLimiterTest {

  private static final String URL_A = "https://confluent.cloud/api/cmk/v2/clusters?environment=env-a";
  private static final String URL_B = "https://confluent.cloud/api/srcm/v3/clusters?environment=env-b";

  /** Build a mock HttpResponse with the given status code and (name, value) header pairs. */
  private static HttpResponse<Buffer> mockResponse(int status, String... headerPairs) {
    @SuppressWarnings("unchecked")
    HttpResponse<Buffer> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(status);
    for (int i = 0; i < headerPairs.length; i += 2) {
      when(response.getHeader(headerPairs[i])).thenReturn(headerPairs[i + 1]);
    }
    return response;
  }

  /** Mock a 200 response with the three X-RateLimit-* headers populated. */
  private static HttpResponse<Buffer> rateLimitResponse(int limit, int remaining, int resetSeconds) {
    return mockResponse(
        200,
        "X-RateLimit-Limit", Integer.toString(limit),
        "X-RateLimit-Remaining", Integer.toString(remaining),
        "X-RateLimit-Reset", Integer.toString(resetSeconds)
    );
  }

  @Test
  void shouldAllowImmediatelyBeforeFirstResponse() {
    // arrange: no server feedback yet, high default rate
    var limiter = new CCloudApiRateLimiter(true, 1000);

    // act
    var start = Instant.now();
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
    var elapsed = Duration.between(start, Instant.now());

    // assert
    assertTrue(
        elapsed.toMillis() < 500,
        "Expected no throttling before first response, but took %dms".formatted(elapsed.toMillis())
    );

    limiter.notifyRequestCompleted(URL_A);
  }

  @Test
  void shouldThrottleWithDefaultRateBeforeServerFeedback() {
    // arrange: 2 permits/sec default
    var limiter = new CCloudApiRateLimiter(true, 2);

    // act: 4 acquires at 2/sec should take roughly 1.5s (3 intervals of 500ms)
    var start = Instant.now();
    for (int i = 0; i < 4; i++) {
      limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(10));
      limiter.notifyRequestCompleted(URL_A);
    }
    var elapsed = Duration.between(start, Instant.now());

    // assert
    assertTrue(
        elapsed.toMillis() >= 500,
        "Expected throttling at default 2/sec, but only took %dms".formatted(elapsed.toMillis())
    );
  }

  @Test
  void shouldAllowFreelyWhenRemainingIsHigh() {
    // arrange: server reports 100 remaining with 60s reset window
    var limiter = new CCloudApiRateLimiter(true, 4);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(URL_A, rateLimitResponse(100, 100, 60));

    // act
    var start = Instant.now();
    for (int i = 0; i < 5; i++) {
      limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
      limiter.recordResponse(URL_A, rateLimitResponse(100, 95 - i, 59));
    }
    var elapsed = Duration.between(start, Instant.now());

    // assert: 5 requests with 100 remaining over 60s should complete well under 5s
    assertTrue(
        elapsed.toMillis() < 5000,
        "Expected fast throughput with high remaining, but took %dms".formatted(elapsed.toMillis())
    );
  }

  @Test
  void shouldThrottleWhenRemainingIsLow() {
    // arrange: server reports only 2 remaining with 10s reset
    var limiter = new CCloudApiRateLimiter(true, 1000);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(URL_A, rateLimitResponse(10, 2, 10));

    // act: next acquire should delay (10s / 3 ≈ 3.3s interval)
    var start = Instant.now();
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(10));
    var elapsed = Duration.between(start, Instant.now());

    // assert
    assertTrue(
        elapsed.toMillis() >= 1000,
        "Expected throttling with low remaining, but only took %dms".formatted(elapsed.toMillis())
    );

    limiter.notifyRequestCompleted(URL_A);
  }

  @Test
  void shouldPauseWhenRemainingIsZero() {
    // arrange: 0 remaining with 2s reset
    var limiter = new CCloudApiRateLimiter(true, 1000);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(URL_A, rateLimitResponse(10, 0, 2));

    // act
    var start = Instant.now();
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(5));
    var elapsed = Duration.between(start, Instant.now());

    // assert: should wait close to the 2s reset window
    assertTrue(
        elapsed.toMillis() >= 1000,
        "Expected ~2s pause with remaining=0, but only took %dms".formatted(elapsed.toMillis())
    );

    limiter.notifyRequestCompleted(URL_A);
  }

  @Test
  void shouldSwitchFromDefaultRateToServerGuided() {
    // arrange
    var limiter = new CCloudApiRateLimiter(true, 4);
    assertNull(limiter.getLatestState(URL_A), "no observed state for this bucket yet");

    // act
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(URL_A, rateLimitResponse(100, 90, 60));

    // assert
    var state = limiter.getLatestState(URL_A);
    assertNotNull(state);
    assertEquals(100, state.limit());
    assertEquals(90, state.remaining());
  }

  @Test
  void shouldRejectNonPositiveDefaultRate() {
    assertThrows(IllegalArgumentException.class, () -> new CCloudApiRateLimiter(true, 0));
    assertThrows(IllegalArgumentException.class, () -> new CCloudApiRateLimiter(true, -1));
  }

  @Test
  void recordResponseShouldUpdateStateWhenHeadersPresent() {
    // arrange
    var limiter = new CCloudApiRateLimiter(true, 4);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));

    // act
    limiter.recordResponse(URL_A, rateLimitResponse(100, 42, 30));

    // assert
    var state = limiter.getLatestState(URL_A);
    assertNotNull(state);
    assertEquals(100, state.limit());
    assertEquals(42, state.remaining());
    assertEquals(30.0, state.resetSeconds());
  }

  @Test
  void recordResponseShouldOnlyDecrementWhenHeadersAbsent() {
    // arrange
    var limiter = new CCloudApiRateLimiter(true, 4);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));

    // act
    limiter.recordResponse(URL_A, mockResponse(200));

    // assert: no headers means no observed state for this bucket
    assertNull(limiter.getLatestState(URL_A));
  }

  @Test
  void recordRateLimitedResponseShouldHonorRetryAfter() {
    // arrange
    var limiter = new CCloudApiRateLimiter(true, 4);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));

    // act
    limiter.recordRateLimitedResponse(URL_A, mockResponse(429, "Retry-After", "7"));

    // assert
    var state = limiter.getLatestState(URL_A);
    assertNotNull(state);
    assertEquals(0, state.remaining());
    assertEquals(7.0, state.resetSeconds());
  }

  @Test
  void recordRateLimitedResponseShouldFallBackWhenRetryAfterAbsent() {
    // arrange
    var limiter = new CCloudApiRateLimiter(true, 4);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));

    // act
    limiter.recordRateLimitedResponse(URL_A, mockResponse(429));

    // assert: falls back to a 1s exhaustion window
    var state = limiter.getLatestState(URL_A);
    assertNotNull(state);
    assertEquals(0, state.remaining());
    assertEquals(1.0, state.resetSeconds());
  }

  @Test
  void parseRetryAfterHeaderShouldReturnMinusOneOnHttpDate() {
    var response = mockResponse(429, "Retry-After", "Wed, 21 Oct 2026 07:28:00 GMT");

    assertEquals(-1, RateLimitState.parseRetryAfterHeader(response));
  }

  @Test
  void bucketsForDifferentUrlsShouldBeIsolated() {
    // arrange: drive bucket A into an exhausted state, leave B untouched
    var limiter = new CCloudApiRateLimiter(true, 1000);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(URL_A, rateLimitResponse(5, 0, 60));

    // act + assert: bucket A is exhausted (60s pause expected); bucket B is still in default mode
    var stateA = limiter.getLatestState(URL_A);
    assertNotNull(stateA);
    assertEquals(0, stateA.remaining());

    assertNull(limiter.getLatestState(URL_B), "B should not be affected by A's response");

    // a B acquire should NOT inherit A's exhaustion - completes near-instantly at default rate
    var start = Instant.now();
    limiter.acquire(URL_B).await().atMost(Duration.ofSeconds(2));
    var elapsed = Duration.between(start, Instant.now());
    assertTrue(
        elapsed.toMillis() < 500,
        "Bucket B should not inherit A's exhaustion; took %dms".formatted(elapsed.toMillis())
    );

    limiter.notifyRequestCompleted(URL_B);

    // bucket count reflects both URLs
    assertEquals(2, limiter.bucketCount());
  }

  @Test
  void bucketsShouldShareBucketKeyAcrossQueryStringVariants() {
    // arrange: same path, different query strings should resolve to the same bucket
    var limiter = new CCloudApiRateLimiter(true, 1000);
    var sameApiDifferentEnv =
        "https://confluent.cloud/api/cmk/v2/clusters?environment=env-different";

    // act
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(URL_A, rateLimitResponse(50, 17, 5));

    // assert: querying state by either URL form returns the same bucket
    var stateA = limiter.getLatestState(URL_A);
    var stateA2 = limiter.getLatestState(sameApiDifferentEnv);
    assertNotNull(stateA);
    assertNotNull(stateA2);
    assertEquals(stateA.remaining(), stateA2.remaining());
    assertEquals(1, limiter.bucketCount());
  }

  @Test
  void shouldTreatLargeResetValueAsAbsoluteEpoch() {
    // arrange: CCloud's /oauth/token has been observed emitting an absolute Unix-epoch Reset.
    // The limiter should normalize that into a small relative seconds value, not interpret 1.78
    // billion as "wait 56 years before the next request".
    var limiter = new CCloudApiRateLimiter(true, 1000);
    long farFutureEpochSeconds = (System.currentTimeMillis() / 1000L) + 30L;
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));

    // act
    limiter.recordResponse(
        URL_A,
        rateLimitResponse(300, 290, (int) farFutureEpochSeconds)
    );

    // assert: stored resetSeconds is now ~30s (relative), not the raw epoch
    var state = limiter.getLatestState(URL_A);
    assertNotNull(state);
    assertTrue(
        state.resetSeconds() < 60,
        "Expected normalized relative reset (~30s), got %.1fs".formatted(state.resetSeconds())
    );
  }

  @Test
  void resetShouldClearAllBuckets() {
    var limiter = new CCloudApiRateLimiter(true, 1000);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(URL_A, rateLimitResponse(5, 0, 60));
    limiter.acquire(URL_B).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(URL_B, rateLimitResponse(20, 19, 1));
    assertEquals(2, limiter.bucketCount());

    limiter.reset();

    assertEquals(0, limiter.bucketCount());
    assertNull(limiter.getLatestState(URL_A));
    assertNull(limiter.getLatestState(URL_B));
  }

  @Test
  void bucketKeyShouldUseUrlPathAndStripApiPrefix() {
    // first-page URL (with /api/ prefix) and pagination next-page URL (without /api/) for the
    // same endpoint family should resolve to the same bucket key
    assertEquals(
        "/cmk/v2/clusters",
        CCloudApiRateLimiter.bucketKey("https://confluent.cloud/api/cmk/v2/clusters?environment=x")
    );
    assertEquals(
        "/cmk/v2/clusters",
        CCloudApiRateLimiter.bucketKey("https://confluent.cloud/cmk/v2/clusters?page_token=abc")
    );
    assertEquals(
        "/srcm/v3/clusters",
        CCloudApiRateLimiter.bucketKey("https://confluent.cloud/api/srcm/v3/clusters")
    );

    // edge cases
    assertEquals("", CCloudApiRateLimiter.bucketKey(null));
    assertEquals("", CCloudApiRateLimiter.bucketKey(""));
    // a path that doesn't start with /api/ is left intact
    assertEquals(
        "/something/else",
        CCloudApiRateLimiter.bucketKey("https://confluent.cloud/something/else")
    );
  }

  @Test
  void inflightOvershootShouldNotTriggerFullWindowPause() {
    // arrange: server reports a healthy non-zero budget (remaining=2 of 5, 1s window)
    var limiter = new CCloudApiRateLimiter(true, 1000);
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(URL_A, rateLimitResponse(5, 2, 1));

    // saturate inflight beyond the server-reported remaining; this mimics the initial fan-out
    // case where many acquires happen before any response feeds updated state back
    for (int i = 0; i < 4; i++) {
      limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(3));
    }

    // act: an additional acquire when inflight far exceeds remaining
    var start = Instant.now();
    limiter.acquire(URL_A).await().atMost(Duration.ofSeconds(3));
    var elapsed = Duration.between(start, Instant.now());

    // assert: pre-fix the limiter would have flipped to "exhausted" and paused the full 1s window
    // (driven by the local inflight counter alone). Now only an explicit server-reported
    // remaining<=0 triggers that branch, so this acquire must be paced - not paused.
    assertTrue(
        elapsed.toMillis() < 900,
        "Inflight overshoot must not trigger full-window pause; took %dms".formatted(
            elapsed.toMillis()
        )
    );
    // server-reported state stays healthy throughout
    var state = limiter.getLatestState(URL_A);
    assertNotNull(state);
    assertTrue(state.remaining() > 0);
  }

  @Test
  void firstPageAndPaginationNextUrlsShouldShareOneBucket() {
    var limiter = new CCloudApiRateLimiter(true, 1000);
    var firstPage = "https://confluent.cloud/api/cmk/v2/clusters?environment=env-a";
    var nextPage  = "https://confluent.cloud/cmk/v2/clusters?page_token=abc";

    limiter.acquire(firstPage).await().atMost(Duration.ofSeconds(2));
    limiter.recordResponse(firstPage, rateLimitResponse(300, 250, 30));

    // a next-page URL with no /api/ prefix sees the same bucket's state
    var stateFromNextPageView = limiter.getLatestState(nextPage);
    assertNotNull(stateFromNextPageView);
    assertEquals(250, stateFromNextPageView.remaining());
    assertEquals(1, limiter.bucketCount());
  }

  @Test
  void disabledLimiterShouldShortCircuitAcquireWithoutBucketCreation() {
    // arrange: 1 req/sec default - if pacing were active, the *second* acquire would block ~1s
    var limiter = new CCloudApiRateLimiter(false, 1);

    // act: two acquires; the second would hit the timeout if the limiter were paced
    limiter.acquire(URL_A).await().atMost(Duration.ofMillis(500));
    limiter.acquire(URL_A).await().atMost(Duration.ofMillis(500));

    // assert: no buckets created, no state observed
    assertEquals(0, limiter.bucketCount());
    assertNull(limiter.getLatestState(URL_A));
  }

  @Test
  void disabledLimiterShouldIgnoreResponseHeaders() {
    // arrange
    var limiter = new CCloudApiRateLimiter(false, 4);

    // act: full record cycle that would normally populate bucket state
    limiter.acquire(URL_A).await().atMost(Duration.ofMillis(100));
    limiter.recordResponse(URL_A, rateLimitResponse(100, 50, 30));
    limiter.recordRateLimitedResponse(URL_B, mockResponse(429, "Retry-After", "1"));
    limiter.notifyRequestCompleted(URL_A);

    // assert: no state retained for either URL
    assertEquals(0, limiter.bucketCount());
    assertNull(limiter.getLatestState(URL_A));
    assertNull(limiter.getLatestState(URL_B));
  }
}
