package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class CCloudHttpSenderTest {

  private static final String URL = "https://api.confluent.cloud/api/check_jwt";

  @Test
  void shouldReleaseTheRateLimiterSlotWhenARequestTimesOut() throws Exception {
    var sender = new CCloudHttpSender();
    sender.rateLimiter = new CCloudApiRateLimiter(true, 1000);
    sender.retryPolicy = mock(CCloudHttpRetryPolicy.class);
    when(sender.retryPolicy.applyRetry(eq(URL), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));

    var result = sender.send(URL, () -> Future.failedFuture(new TimeoutException("stalled")));

    var failure = result
        .map(response -> (Throwable) new AssertionError("expected a failure"))
        .recover(Future::succeededFuture)
        .toCompletionStage()
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    assertInstanceOf(TimeoutException.class, failure);
    assertEquals(0, sender.rateLimiter.inflight(URL));
  }
}
