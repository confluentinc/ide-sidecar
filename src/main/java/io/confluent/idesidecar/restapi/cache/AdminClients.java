package io.confluent.idesidecar.restapi.cache;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;

@ApplicationScoped
public class AdminClients extends Clients<AdminClient> {

  @Inject
  ConnectionStateManager manager;

  public AdminClient getAdminClient(String connectionId) {
    return getClient(
        connectionId,
        "default",
        () -> AdminClient.create(getAdminClientConfig(connectionId))
    );
  }

  private Properties getAdminClientConfig(String connectionId) {
    var spec = manager.getConnectionSpec(connectionId);
    if (spec.bootstrapServers() == null) {
      throw new IllegalArgumentException("No bootstrap servers for connection " + connectionId);
    }

    var props = new Properties();
    props.put("bootstrap.servers", spec.bootstrapServers());
    return props;
  }
}
