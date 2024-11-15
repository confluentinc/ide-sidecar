package io.confluent.idesidecar.restapi.cache;

import static io.confluent.idesidecar.restapi.kafkarest.SchemaManager.SCHEMA_PROVIDERS;
import static jakarta.ws.rs.core.HttpHeaders.AUTHORIZATION;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.SchemaRegistryClusterNotFoundException;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Create an ApplicationScoped bean to cache SchemaRegistryClient instances
 * by connection ID and schema registry client ID.
 */
@ApplicationScoped
public class SchemaRegistryClients extends Clients<SchemaRegistryClient> {
  private static final int SR_CACHE_SIZE = 10;

  @Inject
  ClusterCache clusterCache;

  @Inject
  ClientConfigurator configurator;

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  /**
   * Get a SchemaRegistryClient for the given connection ID and cluster ID. We rely on the
   * sidecar's Schema Registry proxy routes to forward the request to the correct Schema Registry
   * instance.
   */
  public SchemaRegistryClient getClient(String connectionId, String clusterId)
      throws SchemaRegistryClusterNotFoundException {
    // Generate the Schema Registry client configuration
    final var config = configurator.getSchemaRegistryClientConfig(
        connectionId,
        clusterId,
        false
    );
    return getClient(
        connectionId,
        clusterId,
        () -> {
          var headers = Map.of(
              RequestHeadersConstants.CONNECTION_ID_HEADER, connectionId,
              RequestHeadersConstants.CLUSTER_ID_HEADER, clusterId,
              AUTHORIZATION, "Bearer %s".formatted(accessTokenBean.getToken())
          );
          // Create the Schema Registry client
          return createClient(sidecarHost, config, headers);
        });
  }

  private Map<String, Object> getSchemaRegistryClientConfig(String connectionId, String clusterId) {
    try {
      return configurator.getSchemaRegistryClientConfig(
          connectionId,
          clusterId,
          false
      );
    } catch (SchemaRegistryClusterNotFoundException e) {
      Log.errorf(
          "Could not find Schema Registry cluster for connection ID %s and cluster ID %s",
          connectionId, clusterId
      );
      throw new RuntimeException(e);
    }
  }

  public SchemaRegistryClient getClientByKafkaClusterId(
      String connectionId,
      String kafkaClusterId
  ) throws ClusterNotFoundException {
    var srCluster = clusterCache.maybeGetSchemaRegistryForKafkaClusterId(
        connectionId, kafkaClusterId
    );

    if (srCluster.isPresent()) {
      return getClient(connectionId, srCluster.get().id());
    }

    return null;
  }

  private SchemaRegistryClient createClient(
      String srClusterUri,
      Map<String, Object> configurationProperties,
      Map<String, String> headers
  ) {
    return new CachedSchemaRegistryClient(
        Collections.singletonList(srClusterUri),
        SR_CACHE_SIZE,
        SCHEMA_PROVIDERS,
        configurationProperties,
        headers
    );
  }
}
