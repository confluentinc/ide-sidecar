package io.confluent.idesidecar.restapi.kafkarest.controllers;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.context.SessionScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.inject.Inject;
import jakarta.ws.rs.Produces;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;

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
