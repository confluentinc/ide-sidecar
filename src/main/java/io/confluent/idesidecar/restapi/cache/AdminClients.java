package io.confluent.idesidecar.restapi.cache;

import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;

@ApplicationScoped
public class AdminClients extends Clients<AdminClient> {

  private static final String DEFAULT_CLIENT_ID = "default";

  @Inject
  ClusterCache cache;

  public AdminClient getAdminClient(String connectionId, @Nullable String clusterId) {
    if (clusterId != null) {
      return getClient(
          connectionId,
          clusterId,
          () -> AdminClient.create(getAdminClientConfig(connectionId, clusterId))
      );
    } else {
      return getClient(
          connectionId,
          DEFAULT_CLIENT_ID,
          () -> AdminClient.create(getAdminClientConfig(connectionId))
      );
    }
  }

  public Properties getAdminClientConfig(String connectionId, String clusterId) {
    var props = new Properties();
    var cluster = cache.getKafkaCluster(connectionId, clusterId);
    props.put("bootstrap.servers", cluster.bootstrapServers());
    return props;
  }

  /**
   * Get AdminClient from the first available Kafka cluster for the connection. We implicitly assume
   * that there will be only one Kafka cluster per connection.
   * @param connectionId The connection ID
   * @return The AdminClient configuration
   */
  public Properties getAdminClientConfig(String connectionId) {
    // TODO: Define custom exception classes
    var cluster = cache
        .forConnection(connectionId)
        // Accessing a ConcurrentHashMap _may_ be a blocking operation
        .kafkaClusters
        .values()
        .stream()
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException(
                "No Kafka clusters found for connection " + connectionId
            )
        ).spec();

    if (cluster.bootstrapServers() == null) {
      throw new IllegalArgumentException(
          "No bootstrap servers found for Kafka cluster " + cluster.id()
      );
    }

    var props = new Properties();
    props.put("bootstrap.servers", cluster.bootstrapServers());
    return props;
  }
}
