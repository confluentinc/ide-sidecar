package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Strategy for processing requests and responses for a Confluent local Kafka cluster.
 */
@ApplicationScoped
public class ConfluentLocalKafkaClusterStrategy extends ClusterStrategy {

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  /**
   * We require the connection ID header to be passed to our implementation of the Kafka REST API
   * at the /internal/kafka path. This is used to identify the connection that the request is
   * associated with.
   * @param context The context of the proxy request.
   * @return The headers to be passed to our implementation of the Kafka REST API.
   */
  @Override
  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    return HttpHeaders
        .headers()
        .add(CONNECTION_ID_HEADER, context.getConnectionId());
  }

  /**
   * Route the request back to ourselves at the /internal/kafka path.
   * Context: We used to send this proxy request to the Kafka REST server running in the
   * confluent-local container, but now we route the request to our own implementation of the
   * Kafka REST API, served at the /internal/kafka path. This was done to get early feedback
   * on our in-house implementation of the Kafka REST API.
   * @param requestUri The URI of the incoming request.
   * @param clusterUri The Kafka REST URI running alongside the Kafka cluster.
   *                   (unused here)
   * @return The URI of the Kafka REST API running in the sidecar.
   */
  @Override
  public String constructProxyUri(String requestUri, String clusterUri) {
    return uriUtil.combine(
        sidecarHost, requestUri.replaceFirst("^(/kafka|kafka)", "/internal/kafka")
    );
  }

  /**
   * In addition to replacing the cluster URLs with the sidecar host, we also need to replace
   * the internal Kafka REST path /internal/kafka with the external facing /kafka path.
   * @param proxyResponseBody The response body from the Kafka cluster.
   * @return The response body with the internal Kafka REST path replaced with the external path.
   */
  @Override
  public String processProxyResponse(String proxyResponseBody) {
    return super
        .processProxyResponse(proxyResponseBody)
        .replaceAll("/internal/kafka", "/kafka");
  }
}
