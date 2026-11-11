package io.confluent.idesidecar.restapi.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.clients.RetryAfterException;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClient;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClusterProxyRequestProcessorTest {

  private static final String CONNECTION_ID = "direct-1";
  private static final String CLUSTER_ID = "sr-1";

  private final SchemaRegistryClient client = mock(SchemaRegistryClient.class);
  private final ClusterProxyRequestProcessor processor =
      new ClusterProxyRequestProcessor(mock(WebClientFactory.class), mock(Vertx.class));

  @BeforeEach
  void setUp() {
    processor.clients = mock(SchemaRegistryClients.class);
    when(processor.clients.getClient(CONNECTION_ID, CLUSTER_ID)).thenReturn(client);
    processor.httpHeaderExclusions = List.of();
    processor.setNext(new Processor<>() {
      @Override
      public Future<ClusterProxyContext> process(ClusterProxyContext context) {
        return Future.succeededFuture(context);
      }
    });
  }

  @Test
  void shouldAnswer504WhenSchemaRegistryTimesOut() throws Exception {
    failWith(new SocketTimeoutException("Read timed out"));

    var result = processor.process(directSchemaRegistryContext());

    var failure = failureOf(result);
    assertEquals("504", failure.status());
    assertEquals("proxy_timeout", failure.errors().getFirst().code());
    assertEquals("Schema Registry didn't respond in time", failure.title());
  }

  @Test
  void shouldNameTheDeadlineWhenItEndedTheRequest() {
    var failure = ClusterProxyRequestProcessor.toFailure(
        directSchemaRegistryContext(),
        new SocketTimeoutException("Read timed out"),
        Instant.now().minusMillis(1)
    );

    assertEquals("504", failure.status());
    assertEquals("Schema Registry didn't respond within 20s", failure.title());
  }

  @Test
  void shouldAnswer500WhenTheRequestFailsOtherwise() throws Exception {
    failWith(new IOException("Connection reset"));

    var result = processor.process(directSchemaRegistryContext());

    var failure = failureOf(result);
    assertEquals("500", failure.status());
    assertEquals("proxy_error", failure.errors().getFirst().code());
    assertTrue(failure.title().contains("Connection reset"), failure.title());
  }

  @Test
  void shouldPassRetryAfterOnWhenSchemaRegistryStillAsksToWait() throws Exception {
    var tooManyRequests = new RestClientException("Too many requests", 429, 42900);
    failWith(new RetryAfterException(tooManyRequests, Duration.ofSeconds(30), "retries ran out"));

    var context = processor.process(directSchemaRegistryContext()).result();

    assertEquals(429, context.getProxyResponseStatusCode());
    assertEquals("30", context.getProxyResponseHeaders().get("Retry-After"));
  }

  @Test
  void shouldNotAddRetryAfterToOtherSchemaRegistryErrors() throws Exception {
    failWith(new RestClientException("Subject not found", 404, 40401));

    var context = processor.process(directSchemaRegistryContext()).result();

    assertEquals(404, context.getProxyResponseStatusCode());
    assertNull(context.getProxyResponseHeaders().get("Retry-After"));
  }

  private void failWith(Exception e) throws Exception {
    when(client.httpRequest(any(), any(), any(), any(), any())).thenThrow(e);
  }

  private static ClusterProxyContext directSchemaRegistryContext() {
    var context = new ClusterProxyContext(
        "/subjects", MultiMap.caseInsensitiveMultiMap(), HttpMethod.GET, null, Map.of(),
        CONNECTION_ID, CLUSTER_ID, ClusterType.SCHEMA_REGISTRY
    );
    var connection = mock(ConnectionState.class);
    when(connection.getType()).thenReturn(ConnectionType.DIRECT);
    context.setConnectionState(connection);
    context.setProxyRequestMethod(HttpMethod.GET);
    context.setProxyRequestHeaders(MultiMap.caseInsensitiveMultiMap());
    return context;
  }

  private static Failure failureOf(Future<ClusterProxyContext> result) {
    assertTrue(result.failed(), "expected a failed future");
    return assertInstanceOf(ProcessorFailedException.class, result.cause()).getFailure();
  }
}
