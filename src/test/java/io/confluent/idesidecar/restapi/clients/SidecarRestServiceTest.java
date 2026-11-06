package io.confluent.idesidecar.restapi.clients;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SidecarRestServiceTest {

  private static final String OK_BODY = "[\"AVRO\"]";
  private static final Map<String, Object> NO_BACKOFF = Map.of(
      "max.retries", 3,
      "retries.wait.ms", 1,
      "retries.max.wait.ms", 20_000
  );

  private final Deque<Reply> replies = new ArrayDeque<>();
  private final List<Request> requests = new CopyOnWriteArrayList<>();
  private HttpServer server;

  record Reply(int status, String retryAfter) {

  }

  record Request(long nanos, String authorization) {

  }

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void shouldNotRetryBeforeRetryAfterElapses(int status) throws Exception {
    var restService = startService(NO_BACKOFF, new Reply(status, "1"), new Reply(200, null));

    var types = restService.getSchemaTypes();

    assertEquals(List.of("AVRO"), types);
    assertEquals(2, requests.size());
    var gap = Duration.ofNanos(requests.get(1).nanos() - requests.get(0).nanos());
    assertTrue(gap.toMillis() >= 1_000, "retried after only " + gap);
  }

  @Test
  void shouldFailWithinDeadlineWhenRetryAfterIsLonger() throws Exception {
    var restService = startService(NO_BACKOFF, new Reply(429, "30"));
    var started = System.nanoTime();

    var thrown = assertThrows(
        RetryAfterException.class,
        () -> SidecarRestService.withDeadline(
            Instant.now().plusSeconds(5),
            restService::getSchemaTypes
        )
    );

    var elapsed = Duration.ofNanos(System.nanoTime() - started);
    assertTrue(elapsed.toMillis() < 5_000, "took " + elapsed);
    assertEquals(1, requests.size());
    assertEquals(429, thrown.getStatus());
    assertEquals(Duration.ofSeconds(30), thrown.getRetryAfter());
    assertInstanceOf(RestClientException.class, thrown.getSuppressed()[0]);
  }

  @Test
  void shouldFailFastWhenRetryAfterExceedsMaxWaitAndNoDeadline() throws Exception {
    var config = Map.<String, Object>of("retries.wait.ms", 1, "retries.max.wait.ms", 1_000);
    var restService = startService(config, new Reply(503, "30"));

    var thrown = assertThrows(RetryAfterException.class, restService::getSchemaTypes);

    assertEquals(1, requests.size());
    assertEquals(503, thrown.getStatus());
  }

  @Test
  void shouldRetryServerErrorsWithBackoff() throws Exception {
    var restService = startService(
        NO_BACKOFF, new Reply(500, null), new Reply(502, null), new Reply(200, null)
    );

    var types = restService.getSchemaTypes();

    assertEquals(List.of("AVRO"), types);
    assertEquals(3, requests.size());
  }

  @Test
  void shouldThrowRetryAfterExceptionWhenRetriesRunOut() throws Exception {
    var config = Map.<String, Object>of("max.retries", 1, "retries.wait.ms", 1);
    var restService = startService(config, new Reply(429, "1"), new Reply(429, "1"));

    var thrown = assertThrows(RetryAfterException.class, restService::getSchemaTypes);

    assertEquals(2, requests.size());
    assertEquals(Duration.ofSeconds(1), thrown.getRetryAfter());
    assertTrue(thrown.getMessage().contains("retries ran out"), thrown.getMessage());
    assertFalse(thrown.getMessage().contains("error code: 429; error code"), thrown.getMessage());
  }

  @Test
  void shouldNotRetryClientErrors() throws Exception {
    var restService = startService(NO_BACKOFF, new Reply(404, null));

    var thrown = assertThrows(RestClientException.class, restService::getSchemaTypes);

    assertEquals(1, requests.size());
    assertEquals(404, thrown.getStatus());
    assertFalse(thrown instanceof RetryAfterException);
  }

  @Test
  void shouldRethrowOriginalErrorWhenBackoffWouldPassDeadline() throws Exception {
    var config = Map.<String, Object>of("retries.wait.ms", 10_000, "retries.max.wait.ms", 10_000);
    var restService = startService(
        config, new Reply(500, null), new Reply(500, null), new Reply(500, null),
        new Reply(500, null)
    );
    var started = System.nanoTime();

    var thrown = assertThrows(
        RestClientException.class,
        () -> SidecarRestService.withDeadline(
            Instant.now().plusMillis(100),
            restService::getSchemaTypes
        )
    );

    var elapsed = Duration.ofNanos(System.nanoTime() - started);
    assertEquals(RestClientException.class, thrown.getClass());
    assertEquals(500, thrown.getStatus());
    assertTrue(elapsed.toMillis() < 1_000, "took " + elapsed);
  }

  @Test
  void shouldSendCustomHeadersOnEveryAttempt() throws Exception {
    var restService = startService(NO_BACKOFF, new Reply(503, "0"), new Reply(200, null));
    restService.setHttpHeaders(Map.of("Authorization", "Bearer token-1"));

    restService.getSchemaTypes();

    assertEquals(2, requests.size());
    requests.forEach(request -> assertEquals("Bearer token-1", request.authorization()));
  }

  @Test
  void shouldSendBearerTokenOnEveryAttempt() throws Exception {
    var restService = startService(NO_BACKOFF, new Reply(503, null), new Reply(200, null));
    restService.setBearerAuthCredentialProvider(new BearerAuthCredentialProvider() {
      @Override
      public String getBearerToken(URL url) {
        return "oauth-token";
      }

      @Override
      public void configure(Map<String, ?> configs) {
      }
    });

    restService.getSchemaTypes();

    assertEquals(2, requests.size());
    requests.forEach(request -> assertEquals("Bearer oauth-token", request.authorization()));
  }

  @Test
  void shouldUseConfiguredSslSocketFactoryOnEveryAttempt() throws Exception {
    var restService = startService(NO_BACKOFF);
    var httpsService = new SidecarRestService(
        "https://localhost:%d".formatted(server.getAddress().getPort())
    );
    httpsService.configure(NO_BACKOFF);
    var sslSockets = new AtomicInteger();
    httpsService.setSslSocketFactory(new RecordingSslSocketFactory(sslSockets));

    assertThrows(IOException.class, httpsService::getSchemaTypes);

    // every attempt (1 try and 3 retries) goes through the configured factory
    assertTrue(sslSockets.get() >= 4, "factory used " + sslSockets.get() + " times");
  }

  @Test
  void shouldReadRetryAfterAsHttpDate() throws Exception {
    var inAnHour = DateTimeFormatter.RFC_1123_DATE_TIME.format(
        ZonedDateTime.now(ZoneOffset.UTC).plusHours(1)
    );
    var restService = startService(NO_BACKOFF, new Reply(503, inAnHour));

    var thrown = assertThrows(RetryAfterException.class, restService::getSchemaTypes);

    assertEquals(1, requests.size());
    assertTrue(thrown.getRetryAfter().toMinutes() >= 59, "parsed as " + thrown.getRetryAfter());
  }

  @Test
  void shouldRetryIoErrorsUntilRetriesRunOut() throws Exception {
    var restService = startService(NO_BACKOFF);
    server.removeContext("/");
    server.createContext("/", exchange -> {
      requests.add(new Request(System.nanoTime(), null));
      // close without a response, so the client sees an I/O error
      exchange.close();
    });

    assertThrows(IOException.class, restService::getSchemaTypes);

    // 1 try and 3 retries, and the JDK's HTTP client resends a GET once when the server closes
    // the connection without answering
    assertEquals(4 * 2, requests.size());
  }

  @Test
  @Timeout(10)
  void shouldStopWaitingWhenInterrupted() throws Exception {
    var restService = startService(NO_BACKOFF, new Reply(429, "5"), new Reply(200, null));
    var failure = new AtomicReference<Throwable>();
    var stillInterrupted = new AtomicReference<Boolean>();
    var caller = new Thread(() -> {
      try {
        restService.getSchemaTypes();
      } catch (Exception e) {
        failure.set(e);
        stillInterrupted.set(Thread.currentThread().isInterrupted());
      }
    });

    caller.start();
    while (requests.isEmpty()) {
      Thread.sleep(10);
    }
    Thread.sleep(100);
    caller.interrupt();
    caller.join(2_000);

    assertFalse(caller.isAlive(), "still waiting after interrupt");
    assertInstanceOf(InterruptedIOException.class, failure.get());
    assertSame(Boolean.TRUE, stillInterrupted.get());
    assertEquals(1, requests.size());
  }

  /**
   * Counts the sockets the client asks for, then fails them so no TLS handshake is needed.
   */
  private static class RecordingSslSocketFactory extends SSLSocketFactory {

    private final AtomicInteger created;

    RecordingSslSocketFactory(AtomicInteger created) {
      this.created = created;
    }

    private Socket record() throws IOException {
      created.incrementAndGet();
      throw new IOException("recorded");
    }

    @Override
    public Socket createSocket() throws IOException {
      return record();
    }

    @Override
    public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
        throws IOException {
      return record();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
      return record();
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress local, int localPort)
        throws IOException {
      return record();
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
      return record();
    }

    @Override
    public Socket createSocket(InetAddress host, int port, InetAddress local, int localPort)
        throws IOException {
      return record();
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return new String[0];
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return new String[0];
    }
  }

  private SidecarRestService startService(Map<String, Object> config, Reply... scripted)
      throws Exception {
    replies.addAll(List.of(scripted));
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", exchange -> {
      requests.add(new Request(
          System.nanoTime(),
          exchange.getRequestHeaders().getFirst("Authorization")
      ));
      var reply = replies.isEmpty() ? new Reply(500, null) : replies.poll();
      var body = (reply.status() == 200
          ? OK_BODY
          : "{\"error_code\":%1$d,\"message\":\"status %1$d\"}".formatted(reply.status())
      ).getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      if (reply.retryAfter() != null) {
        exchange.getResponseHeaders().add("Retry-After", reply.retryAfter());
      }
      exchange.sendResponseHeaders(reply.status(), body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
    var restService = new SidecarRestService(
        "http://localhost:%d".formatted(server.getAddress().getPort())
    );
    restService.configure(config);
    return restService;
  }
}
