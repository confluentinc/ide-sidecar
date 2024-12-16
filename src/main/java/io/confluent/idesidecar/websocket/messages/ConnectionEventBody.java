package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;

/**
 * Sent by the sidecar to the workspace when a connection is created, when existing connections
 * are updated or their status changed, or when a connection is deleted.
 */
public record ConnectionEventBody(
    @JsonProperty("connection_id") String connectionId,
    @JsonProperty("connection_type") ConnectionType connectionType,
    @JsonProperty("event_kind") EventKind eventKind,
    @JsonProperty("connection_status")ConnectionStatus connectionStatus
) implements MessageBody {

  public enum EventKind {
    CREATED,
    UPDATED,
    DELETED,
    CONNECTED,
    DISCONNECTED
  }

  public ConnectionEventBody(ConnectionState state, EventKind eventKind) {
    this(state.getSpec().id(), state.getSpec().type(), eventKind, state.getStatus());
  }

}