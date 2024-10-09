package io.confluent.idesidecar.restapi.cache;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Properties;
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
  ClusterCache clusterCache;

  @ConfigProperty(name = "ide-sidecar.admin-client-configs")
  Map<String, String> adminClientSidecarConfigs;

  public AdminClients() {
    super(CaffeineSpec.parse(CAFFEINE_SPEC));
  }

  /**
   * Get an AdminClient for the given connection ID and Kafka cluster ID.
   * If the client does not already exist, it will be created.
   * @param connectionId The connection ID
   * @param clusterId The cluster ID
   * @return The AdminClient
   */
  public AdminClient getClient(String connectionId, String clusterId) {
    return getClient(
        connectionId,
        clusterId,
        () -> AdminClient.create(getAdminClientConfig(connectionId, clusterId))
    );
  }

  private Properties getAdminClientConfig(String connectionId, String clusterId) {
    var cluster = clusterCache.getKafkaCluster(connectionId, clusterId);
    var props = new Properties();
    // Set AdminClient configs provided by the sidecar
    props.putAll(adminClientSidecarConfigs);
    props.put("bootstrap.servers", cluster.bootstrapServers());
    return props;
  }

  @Override
  void onConnectionUpdated(@ObservesAsync @Lifecycle.Updated ConnectionState connection) {
    clearClients(connection.getId());
  }
}
