package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.connections.ConnectionState;

public class Connection extends BaseModel<ConnectionSpec> {

  @JsonProperty(required = true)
  protected ConnectionStatus status;

  @JsonProperty(required = true)
  protected ConnectionMetadata metadata;

  public Connection(ConnectionState connectionState, ConnectionStatus status) {
    spec = connectionState.getSpec();
    id = spec.id();
    metadata = connectionState.getConnectionMetadata();
    this.status = status;
  }

  public static Connection from(ConnectionState connectionState) {
    // By default, a connection does not hold any tokens
    return new Connection(connectionState, ConnectionStatus.INITIAL_STATUS);
  }

  public static Connection from(ConnectionState connectionState, ConnectionStatus status) {
    return new Connection(connectionState, status);
  }
}