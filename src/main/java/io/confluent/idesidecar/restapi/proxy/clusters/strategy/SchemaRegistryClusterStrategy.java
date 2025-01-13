package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Strategy for Confluent Cloud Schema Registry clusters. Adds the CCloud data plane authentication
 * headers to the request, along with the `target-sr-cluster` header set to the SR cluster ID.
 */
@ApplicationScoped
public class SchemaRegistryClusterStrategy extends ClusterStrategy {

  @Override
  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    var headers = super.constructProxyHeaders(context);
    headers.addAll(
        context
            .getConnectionState()
            .getSchemaRegistryAuthenticationHeaders(context.getClusterId())
    );
    return headers;
  }
}
