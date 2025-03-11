package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.models.Connection;
import io.quarkus.runtime.annotations.RegisterForReflection;
import javax.validation.constraints.NotNull;

/**
 * Sent by the sidecar to workspaces when a connection is created, when existing connections are
 * updated or their status changed, or when a connection is deleted.
 */
@RegisterForReflection
public record ConnectionEventBody(
    @NotNull @JsonProperty("action") Action action,
    @NotNull @JsonProperty("connection") Connection connection
) implements MessageBody {

  public enum Action {
    CREATED,
    UPDATED,
    DELETED,
    CONNECTED,
    DISCONNECTED
  }

  public ConnectionEventBody(ConnectionState state, Action action) {
    this(action, Connection.from(state));
  }
}
