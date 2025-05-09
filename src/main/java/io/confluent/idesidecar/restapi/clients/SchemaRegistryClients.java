package io.confluent.idesidecar.restapi.clients;

import static io.confluent.idesidecar.restapi.kafkarest.SchemaManager.SCHEMA_PROVIDERS;
import static jakarta.ws.rs.core.HttpHeaders.AUTHORIZATION;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.cache.Clients;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import io.quarkus.logging.Log;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Create an ApplicationScoped bean to cache SchemaRegistryClient instances by connection ID and
 * schema registry client ID.
 */
@ApplicationScoped
public class SchemaRegistryClients extends Clients<SchemaRegistryClient> {

  private static final int SR_CACHE_SIZE = 10;

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  ClusterCache clusterCache;

  @Inject
  ClientConfigurator configurator;

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  /**
   * Get a SchemaRegistryClient for the given connection ID and cluster ID. We rely on the sidecar's
   * Schema Registry proxy routes to forward the request to the correct Schema Registry instance.
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
          var connection = connectionStateManager.getConnectionState(connectionId);
          var schemaRegistryCluster = clusterCache.getSchemaRegistry(connectionId, clusterId);
          var schemaRegistryUri = schemaRegistryCluster.uri();
          var headers = connection.getSchemaRegistryAuthenticationHeaders(clusterId);

          // For CCloud connections, we must point the SchemaRegistryClient to the sidecar-exposed
          // SR proxy endpoints instead of directly interacting with the CCloud SR endpoints for two
          // main reasons:
          //
          // (1) the sidecar-exposed SR proxy gives us access to user-provided HTTP configs, like
          //     custom SSL certs
          // (2) the sidecar-exposed SR proxy supports authentication with long-lived access tokens,
          //     so we won't run into issues caused by the cached SchemaRegistryClient holding
          //     expired CCloud data plane tokens
          if (connection.getType().equals(ConnectionType.CCLOUD)) {
            schemaRegistryUri = sidecarHost;
            headers.addAll(
                Map.of(
                    RequestHeadersConstants.CONNECTION_ID_HEADER, connectionId,
                    RequestHeadersConstants.CLUSTER_ID_HEADER, clusterId,
                    AUTHORIZATION, "Bearer %s".formatted(accessTokenBean.getToken())
                )
            );
          }

          var client = createClient(schemaRegistryUri, config.asMap(), headers);
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
      MultiMap headers
  ) {
    var restService = new RestService(srClusterUri);
    restService.configure(configurationProperties);
    if ("".equals(configurationProperties.get("ssl.endpoint.identification.algorithm"))) {
      // Disable hostname verification
      restService.setHostnameVerifier((hostname, session) -> true);
    }

    var httpHeaders = new HashMap<String, String>();
    headers.forEach(entry -> httpHeaders.put(entry.getKey(), entry.getValue()));
    restService.setHttpHeaders(httpHeaders);

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
