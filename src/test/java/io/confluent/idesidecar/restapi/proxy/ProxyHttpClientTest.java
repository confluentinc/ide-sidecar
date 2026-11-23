package io.confluent.idesidecar.restapi.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.channel.ConnectTimeoutException;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.impl.NoStackTraceTimeoutException;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

// Quarkus, because the code under test logs through io.quarkus.logging.Log
@QuarkusTest
class ProxyHttpClientTest {

  private static final String HOST = "pkc-123.us-west-2.aws.confluent.cloud";
  private static final String URL = "https://" + HOST + "/kafka/v3/x";
  // production's request-timeout-seconds default
  private static final Duration TIMEOUT = Duration.ofSeconds(20);
  // vert.x's message at that timeout
  private static final String REQUEST_TIMEOUT_MESSAGE =
      "The timeout period of 20000ms has been exceeded";
  // netty's message at production's 10s connect-timeout-seconds
  private static final String CONNECT_TIMEOUT_MESSAGE = "connection timed out after 10000 ms";

  @Test
  void toFailureShouldReturnGatewayTimeoutWhenServerStalls() {
    var failure = ProxyHttpClient.toFailure(
        context(URL),
        new NoStackTraceTimeoutException(REQUEST_TIMEOUT_MESSAGE),
        TIMEOUT
    );

    assertEquals("504", failure.status());
    assertEquals("proxy_timeout", failure.errors().get(0).code());
    assertEquals(HOST + " didn't respond within 20s", failure.title());
  }

  @Test
  void toFailureShouldReturnGatewayTimeoutForAWrappedTimeout() {
    var failure = ProxyHttpClient.toFailure(
        context(URL),
        new CompletionException(
            new NoStackTraceTimeoutException(REQUEST_TIMEOUT_MESSAGE)
        ),
        TIMEOUT
    );

    assertEquals("504", failure.status());
    assertEquals("proxy_timeout", failure.errors().get(0).code());
  }

  @Test
  void toFailureShouldReturnGatewayTimeoutWhenConnectTimesOut() {
    var failure = ProxyHttpClient.toFailure(
        context(URL),
        new ConnectTimeoutException(CONNECT_TIMEOUT_MESSAGE),
        TIMEOUT
    );

    assertEquals("504", failure.status());
    assertEquals("proxy_timeout", failure.errors().get(0).code());
    assertEquals("Couldn't connect to " + HOST + " in time", failure.title());
  }

  @Test
  void toFailureShouldReturnServerErrorForOtherFailures() {
    var failure = ProxyHttpClient.toFailure(
        context(URL), new IOException("Connection reset by peer"), TIMEOUT
    );

    assertEquals("500", failure.status());
    assertEquals("proxy_error", failure.errors().get(0).code());
  }

  @Test
  void toFailureShouldNameTheUrlWhenItHasNoParsableHost() {
    var url = "http://localhost:8082/a b";

    var failure = ProxyHttpClient.toFailure(
        context(url),
        new ConnectTimeoutException(CONNECT_TIMEOUT_MESSAGE),
        TIMEOUT
    );

    assertEquals("504", failure.status());
    assertEquals("Couldn't connect to " + url + " in time", failure.title());
  }

  private static ProxyContext context(String url) {
    var context = new ProxyContext(url, null, null, null, null, null);
    context.setProxyRequestAbsoluteUrl(url);
    return context;
  }
}
