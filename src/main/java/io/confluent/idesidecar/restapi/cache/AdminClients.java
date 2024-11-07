package io.confluent.idesidecar.restapi.cache;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClient;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Create an ApplicationScoped bean to cache AdminClient instances by connection ID and client ID.
 */
@ApplicationScoped
public class AdminClients extends Clients<AdminClient> {

  // Evict cached AdminClient instances after 5 minutes of inactivity
  private static final String CAFFEINE_SPEC = "expireAfterAccess=5m";

  @Inject
  ClientConfigurator configurator;

  @ConfigProperty(name = "ide-sidecar.admin-client-configs")
  Map<String, String> adminClientSidecarConfigs;

  public AdminClients() {
    super(CaffeineSpec.parse(CAFFEINE_SPEC));
  }

  /**
   * Get an AdminClient for the given connection ID and Kafka cluster ID.
   * If the client does not already exist, it will be created.
   * @param connectionId The connection ID
   * @param clusterId    The cluster ID
   * @return The AdminClient
   */
  public AdminClient getClient(String connectionId, String clusterId) {
    return getClient(
        connectionId,
        clusterId,
        () -> {
          // Generate the Kafka admin client configuration
          var config = configurator.getAdminClientConfig(connectionId, clusterId, false);
          // Create the admin client
          return AdminClient.create(config);
        }
    );
  }
}
