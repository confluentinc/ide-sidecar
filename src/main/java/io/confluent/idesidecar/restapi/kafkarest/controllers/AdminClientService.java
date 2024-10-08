package io.confluent.idesidecar.restapi.kafkarest.controllers;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;

@ApplicationScoped
public class AdminClientService {
  @Inject
  ConnectionStateManager manager;

  public Properties getAdminClientConfig(String connectionId) {
    var state = manager.getConnectionState(connectionId);
    var props = new Properties();
    props.put(
        "bootstrap.servers",
        state.getSpec().bootstrapServers()
    );
    return props;
  }
}
