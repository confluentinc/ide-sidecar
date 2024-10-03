package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.admin.AdminClient;

@ApplicationScoped
public class CachedAdminClientService {
  record Tuple(String connectionId, String clusterId) {}

  @Inject
  ConnectionStateManager connectionStateManager;

  private final Map<Tuple, AdminClient> cachedAdminClients = new ConcurrentHashMap<>();

  public AdminClient getOrCreateAdminClient(String connectionId, String clusterId) {
    var key = new Tuple(connectionId, clusterId);
    if (!cachedAdminClients.containsKey(key)) {
      var props = connectionStateManager
          .getConnectionSpec(connectionId)
          .getAdminClientConfig();
      cachedAdminClients.put(key, AdminClient.create(props));
    }
    return cachedAdminClients.get(key);
  }
}