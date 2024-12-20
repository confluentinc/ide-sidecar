package io.confluent.idesidecar.websocket.resources;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.idesidecar.websocket.messages.ConnectionEventBody;
import io.confluent.idesidecar.websocket.messages.ConnectionEventBody.Action;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Application bean that broadcasts connection-related events to all workspaces.
 */
@Startup
@Singleton
public class ConnectionEvents {

  @Inject
  WebsocketEndpoint websockets;

  void onConnectionCreated(@ObservesAsync @Lifecycle.Created ConnectionState connection) {
    broadcast(connection, Action.CREATED);
  }

  void onConnectionUpdated(@ObservesAsync @Lifecycle.Updated ConnectionState connection) {
    broadcast(connection, Action.UPDATED);
  }

  void onConnectionEstablished(@ObservesAsync @Lifecycle.Connected ConnectionState connection) {
    broadcast(connection, Action.CONNECTED);
  }

  void onConnectionDisconnected(@ObservesAsync @Lifecycle.Disconnected ConnectionState connection) {
    broadcast(connection, Action.DISCONNECTED);
  }

  void onConnectionDeleted(@ObservesAsync @Lifecycle.Deleted ConnectionState connection) {
    broadcast(connection, Action.DELETED);
  }

  void broadcast(ConnectionState connection, Action action) {
    broadcast(
        new MessageHeaders(MessageType.CONNECTION_EVENT),
        new ConnectionEventBody(connection, action)
    );
  }

  void broadcast(MessageHeaders headers, ConnectionEventBody body) {
    try {
      websockets.broadcast(new Message(headers, body));
    } catch (Exception e) {
      Log.errorf(e, "Failed to broadcast connection event %s", body);
    }
  }
}
