package io.confluent.idesidecar.restapi.clients;

import static io.confluent.idesidecar.restapi.kafkarest.SchemaManager.SCHEMA_PROVIDERS;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.cache.Clients;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
  public SchemaRegistryClient getClient(String connectionId, String clusterId) {
    return getClient(
        connectionId,
        clusterId,
        () -> {
          // Generate the Schema Registry client configuration
          var config = configurator.getSchemaRegistryClientConfig(
              connectionId,
              clusterId
          );
          // Create the Schema Registry client
          var schemaRegistryCluster = clusterCache.getSchemaRegistry(connectionId, clusterId);
          var client = createClient(schemaRegistryCluster.uri(), config.asMap(), Map.of());
          Log.debugf(
              "Created SR client %s for connection %s and cluster %s with configuration:\n  %s",
              client,
              connectionId,
              clusterId,
              config
          );
          return client;
        });
  }

  public SchemaRegistryClient getClientByKafkaClusterId(
      String connectionId,
      String kafkaClusterId
  ) {
    var srCluster = clusterCache.maybeGetSchemaRegistryForKafkaClusterId(
        connectionId, kafkaClusterId
    );

    return srCluster.map(sr -> getClient(connectionId, sr.id())).orElse(null);
  }

  private SchemaRegistryClient createClient(
      String srClusterUri,
      Map<String, Object> configurationProperties,
      Map<String, String> headers
  ) {
    var restService = new RestService(srClusterUri);
    restService.configure(configurationProperties);
    restService.setHttpHeaders(headers);

    var sslFactory = new SslFactory(configurationProperties);
    if (sslFactory.sslContext() != null) {
      restService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
    }

    return new SidecarSchemaRegistryClient(
        restService,
        SR_CACHE_SIZE,
        SCHEMA_PROVIDERS,
        null,
        null
    );
  }
}
