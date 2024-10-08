package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Strategy for processing requests and responses for a local Kafka cluster.
 */
@ApplicationScoped
public class ConfluentLocalKafkaClusterStrategy extends ClusterStrategy {

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  final String confluentLocalKafkaRestHostname;

  public ConfluentLocalKafkaClusterStrategy(
      @ConfigProperty(name = "ide-sidecar.connections.confluent-local.default.kafkarest-hostname")
      String confluentLocalKafkaRestHostname
  ) {
    this.confluentLocalKafkaRestHostname = confluentLocalKafkaRestHostname;
  }

  @Override
  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    return HttpHeaders
        .headers()
        .add(CONNECTION_ID_HEADER, context.getConnectionId());
  }

  @Override
  public String constructProxyUri(String requestUri, String clusterUri) {
    // Remove the /kafka prefix from the request URI since this is how the REST API
    // running in Confluent Local Kafka is configured.
    return uriUtil.combine(
        sidecarHost,
        requestUri.replaceFirst("^(/kafka|kafka)", "/internal/kafka")
    );
  }

  @Override
  public String processProxyResponse(String proxyResponseBody) {
    return super
        .processProxyResponse(proxyResponseBody)
        .replaceAll("/internal/kafka", "/kafka");
  }
}
