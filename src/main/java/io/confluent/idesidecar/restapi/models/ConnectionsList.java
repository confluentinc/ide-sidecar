package io.confluent.idesidecar.restapi.models;

import java.util.List;

public class ConnectionsList extends
    BaseList<Connection> {
  public ConnectionsList(List<Connection> connections) {
    this.data = connections;
  }

  public static ConnectionsList from(List<Connection> connections) {
    return new ConnectionsList(connections);
  }
}
