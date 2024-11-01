package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "api_version",
    "kind",
    "id",
    "metadata",
    "spec",
    "status"
})
@RegisterForReflection
public class Connection extends BaseModel<ConnectionSpec, ConnectionMetadata> {

  /**
   * Create a connection from the given connection state. The metadata and spec are obtained
   * from the connection state, and the status is set to {@link ConnectionStatus#INITIAL_STATUS}.
   *
   * @param connectionState the state of the connection
   * @return the connection
   */
  public static Connection from(
      ConnectionState connectionState
  ) {
    // By default, a connection does not hold any tokens
    return from(connectionState, ConnectionStatus.INITIAL_STATUS);
  }

  /**
   * Create a connection from the given connection state, resource path and status.
   *
   * @param connectionState the state of the connection
   * @param status          the status of the connection
   * @return the connection
   */
  public static Connection from(
      ConnectionState connectionState,
      ConnectionStatus status
  ) {
    return new Connection(
        connectionState.getConnectionMetadata(),
        connectionState.getSpec(),
        status
    );
  }

  protected final ConnectionStatus status;

  @JsonCreator
  public Connection(
      @JsonProperty(value = "metadata") ConnectionMetadata metadata,
      @JsonProperty(value = "spec", required = true) ConnectionSpec spec,
      @JsonProperty(value = "status") ConnectionStatus status
  ) {
    super(spec.id(), metadata, spec);
    this.status = status;
  }

  @JsonProperty(value = "status", required = true)
  public ConnectionStatus status() {
    return status;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Connection that = (Connection) o;
    return Objects.equals(metadata, that.metadata)
        && Objects.equals(spec, that.spec)
        && Objects.equals(status, that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(spec, status, metadata);
  }

  @Override
  public String toString() {
    return "Connection{" + "id='" + id + '\'' + ", metadata=" + metadata
           + ", spec=" + spec + ", status=" + status + '}';
  }
}