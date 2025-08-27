package io.confluent.idesidecar.websocket.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.credentials.BasicCredentials;
import io.confluent.idesidecar.restapi.credentials.Password;
import io.confluent.idesidecar.restapi.models.ConnectionMetadata;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.KafkaClusterConfig;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.SchemaRegistryConfig;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.KafkaClusterStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.SchemaRegistryStatus;
import io.confluent.idesidecar.websocket.messages.ConnectionEventBody;
import io.confluent.idesidecar.websocket.messages.ConnectionEventBody.Action;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
class ConnectionEventsTest extends AbstractWebsocketTestBase {

  private static final Duration TIMEOUT = Duration.ofMillis(500);

  private static final ConnectionSpec SPEC = ConnectionSpec.createDirect(
      "test",
      "Test Connection",
      new KafkaClusterConfig(
          "localhost:9092",
          new BasicCredentials(
              "user",
              new Password("password".toCharArray())
          ),
          null,
          null
      ),
      new SchemaRegistryConfig(
          "my-sr",
          "http://localhost:8081",
          new BasicCredentials(
              "user",
              new Password("password".toCharArray())
          ),
          null
      )
  );
  private static final ConnectionMetadata METADATA = new ConnectionMetadata(
      "http://localhost:1234/gateway/v1/connections/" + SPEC.id(),
      null,
      null
  );

  @Inject
  ConnectionEvents connectionEvents;

  @Test
  void shouldBroadcastConnectionEvents() {
    ConnectedWorkspace sessionWithWorkspace = null;
    ConnectedWorkspace sessionWithoutWorkspace = null;

    try {
      // Given a workspace connected and happy websocket
      sessionWithWorkspace = connectWorkspace(true);
      sessionWithoutWorkspace = connectWorkspace(false);

      // and connections in various states
      var newConnection = connectionWith(ConnectedState.ATTEMPTING);
      var updatedConnection = connectionWith(ConnectedState.ATTEMPTING);
      var connectedConnection = connectionWith(ConnectedState.SUCCESS);
      var disconnectedConnection = connectionWith(ConnectedState.FAILED);

      // When events are triggered
      connectionEvents.onConnectionCreated(newConnection);
      connectionEvents.onConnectionUpdated(updatedConnection);
      connectionEvents.onConnectionEstablished(connectedConnection);
      connectionEvents.onConnectionDisconnected(disconnectedConnection);
      connectionEvents.onConnectionDeleted(disconnectedConnection);

      // Then the workspace with a workspace should receive all messages and no more
      assertEventReceived(sessionWithWorkspace, Action.CREATED, newConnection);
      assertEventReceived(sessionWithWorkspace, Action.UPDATED, updatedConnection);
      assertEventReceived(sessionWithWorkspace, Action.CONNECTED, connectedConnection);
      assertEventReceived(sessionWithWorkspace, Action.DISCONNECTED, disconnectedConnection);
      assertEventReceived(sessionWithWorkspace, Action.DELETED, disconnectedConnection);
      assertNoMoreEventsReceived(sessionWithWorkspace);

      // And the session without a workspace should not receive any messages
      assertNoMoreEventsReceived(sessionWithoutWorkspace);

    } finally {
      closeSafely(sessionWithWorkspace, sessionWithoutWorkspace);
    }
  }

  void assertNoMoreEventsReceived(ConnectedWorkspace session) {
    session.assertNoMessagesAfter(TIMEOUT);
  }

  void assertEventReceived(ConnectedWorkspace session, Action action,
      ConnectionState expectedState) {
    var message = session.waitForMessageOfType(MessageType.CONNECTION_EVENT, TIMEOUT);
    var body = message.body();
    assertNotNull(body);
    var actualBody = assertInstanceOf(ConnectionEventBody.class, body);

    var expectedBody = new ConnectionEventBody(
        expectedState,
        action
    );
    assertEquals(expectedBody, actualBody);
  }

  private ConnectionState connectionWith(ConnectedState state) {
    return connectionWith(statusWith(state));
  }

  private ConnectionState connectionWith(ConnectionStatus status) {
    var connection = Mockito.mock(ConnectionState.class);
    when(connection.getSpec()).thenReturn(SPEC);
    when(connection.getStatus()).thenReturn(status);
    when(connection.getConnectionMetadata()).thenReturn(METADATA);
    return connection;
  }

  private ConnectionStatus statusWith(ConnectedState state) {
    return statusWith(state, state);
  }

  private ConnectionStatus statusWith(ConnectedState kafkaState, ConnectedState srState) {
    return new ConnectionStatus(
        null,
        new KafkaClusterStatus(
            kafkaState,
            null,
            null
        ),
        new SchemaRegistryStatus(
            srState,
            null,
            null
        )
    );
  }
}
