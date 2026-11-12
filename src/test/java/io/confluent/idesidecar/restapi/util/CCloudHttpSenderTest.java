package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CCloudHttpSenderTest {

  private static final String URL = "https://api.confluent.cloud/api/check_jwt";

  @Test
  void shouldReleaseTheRateLimiterSlotWhenARequestTimesOut() throws Exception {
    var sender = new CCloudHttpSender();
    sender.rateLimiter = new CCloudApiRateLimiter(true, 1000);
    sender.retryPolicy = mock(CCloudHttpRetryPolicy.class);
    // a budget that hasn't run out, so the request is sent and then stalls
    when(sender.retryPolicy.budget()).thenReturn(Duration.ofSeconds(40));
    when(sender.retryPolicy.applyRetryWithoutCutoff(eq(URL), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    var result = sender.send(URL, () -> Future.failedFuture(new TimeoutException("stalled")));

    var failure = result
        .map(response -> (Throwable) new AssertionError("expected a failure"))
        .recover(Future::succeededFuture)
        .toCompletionStage()
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    assertInstanceOf(TimeoutException.class, failure);
    // the request's own timeout, not a budget that ran out before it was sent
    assertEquals("stalled", failure.getMessage());
    assertEquals(0, sender.rateLimiter.inflight(URL));
  }

  @Test
  void shouldNotCutOffARequestThatIsAlreadySentWhenTheBudgetRunsOut() throws Exception {
    var sender = new CCloudHttpSender();
    sender.rateLimiter = new CCloudApiRateLimiter(true, 1000);
    sender.retryPolicy = new CCloudHttpRetryPolicy();
    sender.retryPolicy.maxTotalMs = 200;
    HttpResponse<Buffer> ok = mock();
    when(ok.statusCode()).thenReturn(200);
    Promise<HttpResponse<Buffer>> slow = Promise.promise();
    CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS).execute(() -> slow.complete(ok));

    // a token exchange's answer can carry a rotated refresh token, so it mustn't be dropped
    var response = sender.send(URL, slow::future)
        .toCompletionStage()
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);

    assertEquals(200, response.statusCode());
  }

  @Test
  void shouldNotSendARequestOnceItsBudgetHasRunOut() throws Exception {
    var sender = new CCloudHttpSender();
    sender.rateLimiter = new CCloudApiRateLimiter(true, 1000);
    sender.retryPolicy = new CCloudHttpRetryPolicy();
    sender.retryPolicy.maxTotalMs = 200;
    // a 429 pauses the endpoint for 1s, past the 200ms budget
    HttpResponse<Buffer> tooManyRequests = mock();
    when(tooManyRequests.getHeader("Retry-After")).thenReturn("1");
    sender.rateLimiter.recordRateLimitedResponse(URL, tooManyRequests);
    var sent = new AtomicInteger();

    var failure = sender.send(URL, () -> {
          sent.incrementAndGet();
          return Future.failedFuture(new AssertionError("sent past its budget"));
        })
        .map(response -> (Throwable) new AssertionError("expected a failure"))
        .recover(Future::succeededFuture)
        .toCompletionStage()
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);

    assertInstanceOf(TimeoutException.class, failure);
    assertEquals(0, sent.get());
  }
}
