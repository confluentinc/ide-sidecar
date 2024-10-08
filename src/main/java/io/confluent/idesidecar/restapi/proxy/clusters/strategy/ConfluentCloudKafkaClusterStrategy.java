package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Strategy for Confluent Cloud Kafka clusters. Adds the CCloud data plane authentication headers to
 * the request.
 */
@ApplicationScoped
public class ConfluentCloudKafkaClusterStrategy extends ClusterStrategy {
  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  @Override
  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    var headers = HttpHeaders.headers();
    if (context.getConnectionState() instanceof CCloudConnectionState cCloudConnectionState) {
      cCloudConnectionState.getOauthContext()
          .getDataPlaneAuthenticationHeaders()
          .forEach(headers::add);
    }
    return headers;
  }
}
