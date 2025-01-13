package io.confluent.idesidecar.restapi.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.idesidecar.restapi.clients.SchemaRegistryClient;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClients;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class SchemaRegistryClientsTest {

  @Inject
  SchemaRegistryClients clients;

  @Nested
  public class CCloudTests {

    private static final String CONNECTION_ID = "connection-1";

    @AfterEach
    public void after() {
      clients.clearClients(CONNECTION_ID);
    }

    @Test
    public void shouldHaveNoClients() {
      assertNoCachedClients();
    }

    @Test
    public void shouldClearCachedClients() {
      assertNoCachedClients();

      assertClientAdded("1");
      assertClientAdded("2");
      assertClientAdded("3");

      clients.clearClients(CONNECTION_ID);

      assertNoCachedClients();
    }

    @Test
    public void shouldCreateAndReuseClientForSameSchemaRegistryId() {
      // When we create a new client
      var client1 = assertClientAdded("1");

      // Then we get the same client back
      assertClientExists("1", client1);
    }

    @Test
    public void shouldNotReuseClientForDifferentSchemaRegistryId() {
      // When we create a new client
      var client1 = assertClientAdded("1");

      // Then we get the same client back
      assertClientExists("1", client1);
      assertClientExists("1", client1);

      // When we create a different client
      var client2 = assertClientAdded("2");

      // Then we still can get both clients
      assertClientExists("1", client1);
      assertClientExists("2", client2);
    }

    SchemaRegistryClient assertClientAdded(String id) {
      AtomicBoolean called = new AtomicBoolean(false);
      var preCount = clients.clientCount();
      var result = clients.getClient(CONNECTION_ID, id, () -> {
        called.set(true);
        return new MockSchemaRegistryClient();
      });
      assertTrue(called.get());
      assertEquals(preCount + 1, clients.clientCount());
      return result;
    }

    void assertClientExists(String id, SchemaRegistryClient expected) {
      var result = clients.getClient(
          CONNECTION_ID,
          id,
          () -> fail("Did not expect to create a new client")
      );
      assertSame(expected, result);
    }

    void assertNoCachedClients() {
      assertEquals(0, clients.clientCount(CONNECTION_ID));
    }
  }
}
