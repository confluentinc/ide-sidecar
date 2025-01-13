package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Strategy for Confluent Cloud Kafka clusters. Adds the CCloud data plane authentication headers to
 * the request.
 */
@ApplicationScoped
public class ConfluentCloudKafkaClusterStrategy extends ClusterStrategy {

  @Override
  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    var headers = super.constructProxyHeaders(context);
    headers.addAll(
        context
            .getConnectionState()
            .getKafkaAuthenticationHeaders()
    );
    return headers;
  }
}
