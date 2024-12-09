package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Strategy for forwarding incoming REST requests to the Schema Registry specified on a direct
 * connection.
 */
@ApplicationScoped
public class DirectSchemaRegistryClusterStrategy extends ClusterStrategy {

  private static final String TARGET_SR_CLUSTER_HEADER = "target-sr-cluster";

  /**
   * Constructs the headers for the proxied request, and add the authentication headers from the
   * credentials, and the `target-sr-cluster` header set to the connection's SR cluster ID.
   *
   * @param context the context of the proxy request
   * @return the headers to be used in the proxy request to the Schema Registry
   */
  @Override
  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    var headers = super.constructProxyHeaders(context);
    if (context.getConnectionState() instanceof DirectConnectionState directConnectionState) {
      var srConfig = directConnectionState.getSpec().schemaRegistryConfig();
      if (srConfig != null) {
        var credentials = srConfig.credentials();
        if (credentials != null) {
          credentials.httpClientHeaders().ifPresent(map -> map.forEach(headers::add));
        }
      }
    }
    headers.add(TARGET_SR_CLUSTER_HEADER, context.getClusterId());

    return headers;
  }
}

