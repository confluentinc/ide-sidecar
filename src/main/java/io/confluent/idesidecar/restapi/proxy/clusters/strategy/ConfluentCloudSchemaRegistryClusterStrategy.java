package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Strategy for Confluent Cloud Schema Registry clusters. Adds the CCloud data plane authentication
 * headers to the request, along with the `target-sr-cluster` header set to the SR cluster ID.
 */
@ApplicationScoped
public class ConfluentCloudSchemaRegistryClusterStrategy extends ClusterStrategy {

  private static final String TARGET_SR_CLUSTER_HEADER = "target-sr-cluster";

  @Override
  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    var headers = super.constructProxyHeaders(context);
    if (context.getConnectionState() instanceof CCloudConnectionState cCloudConnectionState) {
      cCloudConnectionState.getOauthContext()
          .getDataPlaneAuthenticationHeaders()
          .forEach(headers::add);
    }
    headers.add(TARGET_SR_CLUSTER_HEADER, context.getClusterId());

    return headers;
  }
}
