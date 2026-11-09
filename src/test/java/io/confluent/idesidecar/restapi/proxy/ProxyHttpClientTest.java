package io.confluent.idesidecar.restapi.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.channel.ConnectTimeoutException;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.impl.NoStackTraceTimeoutException;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

// Quarkus, because the code under test logs through io.quarkus.logging.Log
@QuarkusTest
class ProxyHttpClientTest {

  private static final String HOST = "pkc-123.us-west-2.aws.confluent.cloud";
  private static final String URL = "https://" + HOST + "/kafka/v3/x";
  private static final Duration TIMEOUT = Duration.ofSeconds(20);

  @Test
  void toFailureShouldReturnGatewayTimeoutWhenServerStalls() {
    var failure = ProxyHttpClient.toFailure(
        context(URL),
        new NoStackTraceTimeoutException("The timeout period of 20000ms has been exceeded"),
        TIMEOUT
    );

    assertEquals("504", failure.status());
    assertEquals("proxy_timeout", failure.errors().get(0).code());
    assertEquals(HOST + " didn't respond within 20s", failure.title());
  }

  @Test
  void toFailureShouldReturnGatewayTimeoutWhenConnectTimesOut() {
    var failure = ProxyHttpClient.toFailure(
        context(URL),
        new ConnectTimeoutException("connection timed out after 60000 ms"),
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
        new ConnectTimeoutException("connection timed out after 60000 ms"),
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
