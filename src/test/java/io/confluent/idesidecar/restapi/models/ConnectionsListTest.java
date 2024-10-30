package io.confluent.idesidecar.restapi.models;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.deserializeAndSerialize;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.serializeAndDeserialize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ConnectionsListTest {

  @Test
  void shouldCreateEmptyListWithMetadata() {
    var connectionsList = new ConnectionsList();
    assertEquals(0, connectionsList.data().size());

    assertNotNull(connectionsList.metadata());
    assertNotNull(connectionsList.metadata().self());
    assertNull(connectionsList.metadata().next());
    assertEquals(0, connectionsList.metadata().totalSize());
  }

  @Test
  void shouldSerializeAndDeserializeEmptyList() {
    var connectionsList = new ConnectionsList();
    serializeAndDeserialize(connectionsList);
  }

  @Test
  void shouldDeserializeAndSerializeEmptyList() {
    deserializeAndSerialize(
        "connections/empty-list-connections-response.json",
        ConnectionsList.class
    );
  }

  @Test
  void shouldDeserializeAndSerializeNonEmptyList() {
    deserializeAndSerialize(
        "connections/list-connections-response.json",
        ConnectionsList.class
    );
  }
}
