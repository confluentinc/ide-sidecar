package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.models.Connection;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Sent by the sidecar to the workspace when a connection is created, when existing connections
 * are updated or their status changed, or when a connection is deleted.
 */
@RegisterForReflection
public record ConnectionEventBody(
    @JsonProperty("event_kind") EventKind eventKind,
    @JsonProperty("connection") Connection connection
) implements MessageBody {

  public enum EventKind {
    CREATED,
    UPDATED,
    DELETED,
    CONNECTED,
    DISCONNECTED
  }

  public ConnectionEventBody(ConnectionState state, EventKind eventKind) {
    this(eventKind, Connection.from(state));
  }
}
