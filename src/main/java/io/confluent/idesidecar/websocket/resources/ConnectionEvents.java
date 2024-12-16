/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.websocket.resources;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.idesidecar.websocket.messages.ConnectionEventBody;
import io.confluent.idesidecar.websocket.messages.ConnectionEventBody.EventKind;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

/**
 * Application bean that broadcasts connection-related events to all workspaces.
 */
@ApplicationScoped
public class ConnectionEvents {

  @Inject
  WebsocketEndpoint websockets;

  void onConnectionCreated(@ObservesAsync @Lifecycle.Created ConnectionState connection) {
    broadcast(connection, EventKind.CREATED);
  }

  void onConnectionUpdated(@ObservesAsync @Lifecycle.Updated ConnectionState connection) {
    broadcast(connection, EventKind.UPDATED);
  }

  void onConnectionEstablished(@ObservesAsync @Lifecycle.Connected ConnectionState connection) {
    broadcast(connection, EventKind.CONNECTED);
  }

  void onConnectionDisconnected(@ObservesAsync @Lifecycle.Disconnected ConnectionState connection) {
    broadcast(connection, EventKind.DISCONNECTED);
  }

  void onConnectionDeleted(@ObservesAsync @Lifecycle.Deleted ConnectionState connection) {
    broadcast(connection, EventKind.DELETED);
  }

  void broadcast(ConnectionState connection, EventKind kind) {
    broadcast(
        new ConnectionEventBody(connection, kind),
        new MessageHeaders(MessageType.CONNECTION_EVENT)
    );
  }

  void broadcast(ConnectionEventBody body, MessageHeaders headers) {
    try {
      websockets.broadcast(new Message(headers, body));
    } catch (Exception e) {
      Log.errorf("Failed to broadcast connection event %s", body);
    }
  }
}
