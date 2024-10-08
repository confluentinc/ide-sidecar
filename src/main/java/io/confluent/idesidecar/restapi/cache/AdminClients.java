package io.confluent.idesidecar.restapi.cache;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;

@ApplicationScoped
public class AdminClients extends Clients<AdminClient> {

  private static final String DEFAULT_CLIENT_ID = "default";

  @Inject
  ConnectionStateManager manager;

  public AdminClient getAdminClient(String connectionId) {
    return getClient(
        connectionId,
        // We expect to have only one AdminClient per connection
        DEFAULT_CLIENT_ID,
        () -> AdminClient.create(getAdminClientConfig(connectionId)));
  }

  public Properties getAdminClientConfig(String connectionId) {
    var props = new Properties();
    var connection = manager.getConnectionState(connectionId).getSpec();
    props.put(
        "bootstrap.servers",
        connection.bootstrapServers()
    );
    return props;
  }
}
