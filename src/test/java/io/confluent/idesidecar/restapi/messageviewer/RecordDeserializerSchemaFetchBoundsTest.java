package io.confluent.idesidecar.restapi.messageviewer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpServer;
import io.confluent.idesidecar.restapi.clients.SchemaErrors;
import io.confluent.idesidecar.restapi.clients.SidecarSchemaRegistryClient;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.quarkus.test.junit.QuarkusTest;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logmanager.ExtLogRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Bounds on the schema fetch behind the message viewer: the sidecar retries each fetch, and the
 * Schema Registry client retries each of those attempts again. Runs as a Quarkus test so that
 * {@code Log} calls reach the JBoss log manager the log assertions read from.
 */
@QuarkusTest
class RecordDeserializerSchemaFetchBoundsTest {

  private static final String CONNECTION_ID = "schema-fetch-bounds";
  private static final int SCHEMA_ID = 10008;
  // magic byte 0, then schema ID 10008
  private static final byte[] RECORD_WITH_SCHEMA_ID = new byte[]{0, 0, 0, 39, 24};
  // the sidecar doesn't override the Schema Registry client's max.retries
  private static final int CLIENT_SENDS_PER_ATTEMPT =
      1 + SchemaRegistryClientConfig.MAX_RETRIES_DEFAULT;
  // application.yml default for ide-sidecar.schema-fetch-retry.max-retries
  private static final int PRODUCTION_MAX_RETRIES = 5;
  // long enough that only the retry limits end the request-cap tests
  private static final long GENEROUS_TIMEOUT_MS = 10_000;

  @ConfigProperty(name = "ide-sidecar.schema-fetch-retry.max-retries")
  int testProfileMaxRetries;

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

  @BeforeEach
  void captureLogs() {
    schemaErrors.clearByConnectionId(CONNECTION_ID);
    deserializerLogger.addHandler(logCapture);
  }

  @AfterEach
  void releaseLogs() {
    deserializerLogger.removeHandler(logCapture);
    schemaErrors.clearByConnectionId(CONNECTION_ID);
  }

  @Test
  void shouldCapRequestsForFailingSchemaIdInTestProfile() throws Exception {
    assertRequestCap(testProfileMaxRetries, 16);
  }

  @Test
  void shouldCapRequestsForFailingSchemaIdWithProductionRetries() throws Exception {
    assertRequestCap(PRODUCTION_MAX_RETRIES, 24);
  }

  @Test
  void shouldStopWaitingForSchemaFetchAfterTimeout() throws Exception {
    var neverReleased = new CountDownLatch(1);
    var client = mock(CachedSchemaRegistryClient.class);
    when(client.getSchemaById(anyInt())).thenAnswer(invocation -> {
      neverReleased.await();
      return null;
    });
    var deserializer = new RecordDeserializer(1, 1, 3, 200, schemaErrors);

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
  void shouldStopRetryingAndReportLastErrorAfterTimeout() throws Exception {
    var calls = new AtomicInteger();
    var client = mock(CachedSchemaRegistryClient.class);
    when(client.getSchemaById(anyInt())).thenAnswer(invocation -> {
      calls.incrementAndGet();
      Thread.sleep(50);
      throw new RestClientException("Too Many Requests", 429, 429);
    });
    var deserializer = new RecordDeserializer(1, 1, 100, 300, schemaErrors);

    var result = deserializer.deserialize(RECORD_WITH_SCHEMA_ID, client, context, false);
    var callsAtTimeout = calls.get();
    Thread.sleep(500);

    // at most the attempt already running when the timeout fired
    assertTrue(calls.get() <= callsAtTimeout + 1, "kept retrying: " + calls.get());
    assertTrue(
        result.errorMessage().contains("timed out after 0.3s; last error: Too Many Requests"),
        result.errorMessage()
    );
  }

  private void assertRequestCap(int maxRetries, int expectedRequests) throws Exception {
    var requests = new AtomicInteger();
    var server = alwaysTooManyRequests(requests);
    var deserializer = new RecordDeserializer(1, 1, maxRetries, GENEROUS_TIMEOUT_MS, schemaErrors);

    try {
      var client = clientWithoutRetryWaits(server.getAddress().getPort());

      var result = deserializer.deserialize(RECORD_WITH_SCHEMA_ID, client, context, false);

      assertEquals(expectedRequests, (1 + maxRetries) * CLIENT_SENDS_PER_ATTEMPT);
      assertEquals(expectedRequests, requests.get());
      assertTrue(
          result.errorMessage().contains("gave up after %d attempts".formatted(1 + maxRetries)),
          result.errorMessage()
      );
      assertLoggedAtErrorContaining("gave up after %d attempts".formatted(1 + maxRetries));
    } finally {
      server.stop(0);
    }
  }

  private static HttpServer alwaysTooManyRequests(AtomicInteger requests) throws Exception {
    var server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    var body = "{\"error_code\":429,\"message\":\"Too Many Requests\"}"
        .getBytes(StandardCharsets.UTF_8);
    server.createContext("/", exchange -> {
      requests.incrementAndGet();
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(429, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
    return server;
  }

  /**
   * Build a client with the default retry count but no waits between retries, so the test counts
   * every request without waiting out the client's backoff.
   */
  private static SidecarSchemaRegistryClient clientWithoutRetryWaits(int port) {
    var restService = new RestService("http://localhost:%d".formatted(port));
    restService.configure(Map.of("retries.wait.ms", 0, "retries.max.wait.ms", 0));
    return new SidecarSchemaRegistryClient(restService, 10, Map.of());
  }

  private void assertLoggedAtErrorContaining(String text) {
    var matched = logRecords.stream()
        .filter(r -> r.getLevel().intValue() >= Level.SEVERE.intValue())
        .filter(r -> r.getThrown() != null)
        .map(r -> r instanceof ExtLogRecord ext ? ext.getFormattedMessage() : r.getMessage())
        .anyMatch(message -> message != null && message.contains(text));
    assertTrue(matched, "no ERROR log with a throwable containing '%s'".formatted(text));
  }
}
