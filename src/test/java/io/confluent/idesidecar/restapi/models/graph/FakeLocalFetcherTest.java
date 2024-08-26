package io.confluent.idesidecar.restapi.models.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FakeLocalFetcherTest {

  private FakeLocalFetcher fetcher;

  @BeforeEach
  public void beforeEach() {
    fetcher = new FakeLocalFetcher();
  }

  @Test
  void shouldGetOneConnection() {
    var connections = fetcher.getConnections();
    assertEquals(1, connections.size());
    var connection = connections.get(0);
    assertEquals(FakeLocalFetcher.CONNECTION_ID, connection.getId());
    assertEquals("local", connection.getName());
    assertEquals(ConnectionType.LOCAL, connection.getType());
  }

  @Test
  void getKafkaCluster() {
    var cluster = fetcher.getKafkaCluster("fake-connection")
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .assertCompleted()
            .getItem();

    assertEquals("confluent-local",cluster.id());
    assertEquals("localhost:9092", cluster.bootstrapServers());
    assertEquals("http://localhost:8082", cluster.uri());
  }

  @Test
  void getSchemaRegistry() {
    var cluster = fetcher
        .getSchemaRegistry(FakeLocalFetcher.CONNECTION_ID)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .assertCompleted()
        .getItem();
    assertEquals("http://localhost:8080", cluster.uri());
  }
}