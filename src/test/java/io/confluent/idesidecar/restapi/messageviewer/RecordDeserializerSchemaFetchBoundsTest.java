package io.confluent.idesidecar.restapi.messageviewer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpServer;
import io.confluent.idesidecar.restapi.clients.SchemaErrors;
import io.confluent.idesidecar.restapi.clients.SidecarRestService;
import io.confluent.idesidecar.restapi.clients.SidecarSchemaRegistryClient;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.quarkus.test.junit.QuarkusTest;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.jboss.logmanager.ExtLogRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Bounds on the schema fetch behind the message viewer: the Schema Registry client owns the
 * retries, and the viewer's timeout bounds them. Runs as a Quarkus test so that
 * {@code Log} calls reach the JBoss log manager the log assertions read from.
 */
@QuarkusTest
class RecordDeserializerSchemaFetchBoundsTest {

  private static final String CONNECTION_ID = "schema-fetch-bounds";
  private static final int SCHEMA_ID = 10008;
  // magic byte 0, then schema ID 10008
  private static final byte[] RECORD_WITH_SCHEMA_ID = new byte[]{0, 0, 0, 39, 24};
  // the sidecar doesn't override the Schema Registry client's max.retries
  private static final int CLIENT_SENDS_PER_FETCH =
      1 + SchemaRegistryClientConfig.MAX_RETRIES_DEFAULT;
  // long enough that only the retry limits end the request-cap tests
  private static final long GENEROUS_TIMEOUT_MS = 10_000;

  private final KafkaRestProxyContext context = new KafkaRestProxyContext(
      null, null, null, null, null, CONNECTION_ID, "cluster-1", "topic-1"
  );
  private final SchemaErrors schemaErrors = new SchemaErrors();
  private final List<LogRecord> logRecords = new CopyOnWriteArrayList<>();
  private final Handler logCapture = new Handler() {
    @Override
    public void publish(LogRecord record) {
      logRecords.add(record);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
    }
  };
  private final Logger deserializerLogger = Logger.getLogger(RecordDeserializer.class.getName());
  private final Logger restServiceLogger = Logger.getLogger(SidecarRestService.class.getName());
  private final Logger mutinyLogger =
      Logger.getLogger("io.quarkus.mutiny.runtime.MutinyInfrastructure");

  @BeforeEach
  void captureLogs() {
    schemaErrors.clearByConnectionId(CONNECTION_ID);
    deserializerLogger.addHandler(logCapture);
    restServiceLogger.addHandler(logCapture);
    mutinyLogger.addHandler(logCapture);
  }

  @AfterEach
  void releaseLogs() {
    deserializerLogger.removeHandler(logCapture);
    restServiceLogger.removeHandler(logCapture);
    mutinyLogger.removeHandler(logCapture);
    schemaErrors.clearByConnectionId(CONNECTION_ID);
  }

  @Test
  void shouldLeaveRetriesToTheSchemaRegistryClient() throws Exception {
    var requests = new AtomicInteger();
    var server = alwaysTooManyRequests(requests);
    var deserializer = new RecordDeserializer(GENEROUS_TIMEOUT_MS, schemaErrors);

    try {
      var client = clientWithoutRetryWaits(server.getAddress().getPort());

      var result = deserializer.deserialize(RECORD_WITH_SCHEMA_ID, client, context, false);

      // 1 attempt x 4 sends (1 + the client's 3 retries), with no sidecar retries on top
      assertEquals(CLIENT_SENDS_PER_FETCH, requests.get());
      assertTrue(result.errorMessage().contains("gave up: "), result.errorMessage());
      assertLoggedAtErrorContaining("gave up: ");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void shouldStopWaitingForSchemaFetchAfterTimeout() throws Exception {
    var neverReleased = new CountDownLatch(1);
    var client = mock(CachedSchemaRegistryClient.class);
    when(client.getSchemaById(anyInt())).thenAnswer(invocation -> {
      neverReleased.await();
      return null;
    });
    var deserializer = new RecordDeserializer(200, schemaErrors);

    try {
      var started = System.nanoTime();
      var result = assertTimeoutPreemptively(
          Duration.ofSeconds(5),
          () -> deserializer.deserialize(RECORD_WITH_SCHEMA_ID, client, context, false)
      );
      var elapsed = Duration.ofNanos(System.nanoTime() - started);
      var cachedResult = deserializer.deserialize(RECORD_WITH_SCHEMA_ID, client, context, false);

      assertTrue(elapsed.toMillis() >= 200, "returned before the timeout: " + elapsed);
      assertTrue(elapsed.toMillis() < 2_000, "returned long after the timeout: " + elapsed);
      assertTrue(result.errorMessage().contains("timed out after 0.2s"), result.errorMessage());
      assertEquals(result.errorMessage(), cachedResult.errorMessage());
      verify(client, times(1)).getSchemaById(SCHEMA_ID);
      assertLoggedAtErrorContaining("timed out after 0.2s");
    } finally {
      neverReleased.countDown();
    }
  }

  @Test
  void shouldFailWithoutRetryingWhenRetryAfterOutlastsTheTimeout() throws Exception {
    // 15s is under the client's 20s limit on Retry-After, but not under the 5s timeout here
    var requests = new AtomicInteger();
    var server = alwaysTooManyRequests(requests, "15");
    var deserializer = new RecordDeserializer(5_000, schemaErrors);

    try {
      var restService = new SidecarRestService(
          "http://localhost:%d".formatted(server.getAddress().getPort())
      );
      restService.configure(Map.of());
      var client = new SidecarSchemaRegistryClient(restService, 10, Map.of());
      var started = System.nanoTime();

      var result = deserializer.deserialize(RECORD_WITH_SCHEMA_ID, client, context, false);

      var elapsed = Duration.ofNanos(System.nanoTime() - started);
      assertTrue(elapsed.toMillis() < 5_000, "took " + elapsed);
      // 1 send: waiting out the 15s Retry-After would pass the 5s deadline
      assertEquals(1, requests.get());
      assertTrue(
          result.errorMessage().contains("asked to retry after 15s"),
          result.errorMessage()
      );
      assertFalse(result.errorMessage().contains("gave up"), result.errorMessage());
      assertLoggedAtErrorContaining("asked to retry after 15s");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void shouldEndATricklingSchemaFetchAtTheTimeout() throws Exception {
    var clientGone = new CountDownLatch(1);
    var server = tricklingSchema(Duration.ofSeconds(5), clientGone);
    var deserializer = new RecordDeserializer(500, schemaErrors);

    try {
      var client = clientWithoutRetryWaits(server.getAddress().getPort());
      var started = System.nanoTime();

      var result = deserializer.deserialize(RECORD_WITH_SCHEMA_ID, client, context, false);

      var elapsed = Duration.ofNanos(System.nanoTime() - started);
      assertTrue(elapsed.toMillis() < 1_500, "took " + elapsed);
      assertNotNull(result.errorMessage());
      // the fetch itself stopped reading, not just the wait for it
      assertTrue(
          clientGone.await(1, TimeUnit.SECONDS),
          "the schema fetch kept reading after the timeout"
      );
      // the fetch's own failure comes after the timeout, so it shouldn't be logged as dropped
      Thread.sleep(300);
      assertTrue(
          logRecords.stream().noneMatch(r -> r.getLoggerName().equals(mutinyLogger.getName())),
          "logged a dropped failure"
      );
    } finally {
      server.stop(0);
    }
  }

  @Test
  void shouldLogAFetchFailureThatArrivesAfterTheTimeout() throws Exception {
    var timedOut = new CountDownLatch(1);
    var client = mock(CachedSchemaRegistryClient.class);
    when(client.getSchemaById(anyInt())).thenAnswer(invocation -> {
      timedOut.await();
      throw new RestClientException("Schema not found", 404, 40403);
    });
    var deserializer = new RecordDeserializer(200, schemaErrors);

    deserializer.deserialize(RECORD_WITH_SCHEMA_ID, client, context, false);
    timedOut.countDown();

    var deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (!loggedAtErrorContaining("Schema not found") && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertLoggedAtErrorContaining("failed after its 0.2s timeout: Schema not found");
  }

  @Test
  void shouldLogALateFailureOnceWhenTheClientAlreadyGaveUpOnIt() throws Exception {
    var timedOut = new CountDownLatch(1);
    // nothing listens on port 1; the deadline has passed by the time this is called anyway
    var restService = new SidecarRestService("http://localhost:1");
    restService.configure(Map.of("max.retries", 0));
    var client = mock(CachedSchemaRegistryClient.class);
    when(client.getSchemaById(anyInt())).thenAnswer(invocation -> {
      timedOut.await();
      restService.getSchemaTypes();
      return null;
    });
    var deserializer = new RecordDeserializer(200, schemaErrors);

    deserializer.deserialize(RECORD_WITH_SCHEMA_ID, client, context, false);
    timedOut.countDown();

    var deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
    while (!loggedAtErrorContaining("/schemas/types") && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    Thread.sleep(200);
    var entries = logRecords.stream()
        .filter(r -> r.getLevel().intValue() >= Level.SEVERE.intValue())
        .map(r -> r instanceof ExtLogRecord ext ? ext.getFormattedMessage() : r.getMessage())
        .filter(message -> message != null && message.contains("/schemas/types"))
        .toList();
    assertEquals(1, entries.size(), String.valueOf(entries));
  }

  private static HttpServer alwaysTooManyRequests(AtomicInteger requests) throws Exception {
    return alwaysTooManyRequests(requests, null);
  }

  private static HttpServer alwaysTooManyRequests(AtomicInteger requests, String retryAfter)
      throws Exception {
    var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    var body = "{\"error_code\":429,\"message\":\"Too Many Requests\"}"
        .getBytes(StandardCharsets.UTF_8);
    server.createContext("/", exchange -> {
      requests.incrementAndGet();
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      if (retryAfter != null) {
        exchange.getResponseHeaders().add("Retry-After", retryAfter);
      }
      exchange.sendResponseHeaders(429, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
    return server;
  }

  /**
   * A Schema Registry whose schema responses arrive a byte at a time over {@code duration}.
   * {@code clientGone} counts down when the client stops reading.
   */
  private static HttpServer tricklingSchema(Duration duration, CountDownLatch clientGone)
      throws Exception {
    var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", exchange -> {
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, 0);
      try (var body = exchange.getResponseBody()) {
        body.write('{');
        body.flush();
        var end = System.nanoTime() + duration.toNanos();
        while (System.nanoTime() < end) {
          Thread.sleep(100);
          body.write(' ');
          body.flush();
        }
        body.write("\"schema\":\"\\\"string\\\"\"}".getBytes(StandardCharsets.UTF_8));
      } catch (IOException e) {
        clientGone.countDown();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    server.start();
    return server;
  }

  /**
   * Build a client with the default retry count but no waits between retries, so the test counts
   * every request without waiting out the client's backoff.
   */
  private static SidecarSchemaRegistryClient clientWithoutRetryWaits(int port) {
    var restService = new SidecarRestService("http://localhost:%d".formatted(port));
    restService.configure(Map.of("retries.wait.ms", 0, "retries.max.wait.ms", 0));
    return new SidecarSchemaRegistryClient(restService, 10, Map.of());
  }

  private void assertLoggedAtErrorContaining(String text) {
    assertTrue(
        loggedAtErrorContaining(text),
        "no ERROR log with a throwable containing '%s'".formatted(text)
    );
  }

  private boolean loggedAtErrorContaining(String text) {
    return logRecords.stream()
        .filter(r -> r.getLevel().intValue() >= Level.SEVERE.intValue())
        .filter(r -> r.getThrown() != null)
        .map(r -> r instanceof ExtLogRecord ext ? ext.getFormattedMessage() : r.getMessage())
        .anyMatch(message -> message != null && message.contains(text));
  }
}
