package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Strategy for processing requests and responses for a Kafka cluster.
 */
@ApplicationScoped
public class DirectKafkaClusterStrategy extends ClusterStrategy {

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  @Inject
  SidecarAccessTokenBean accessToken;

  /**
   * We require the connection ID header to be passed to our implementation of the Kafka REST API
   * at the /internal/kafka path. This is used to identify the connection that the request is
   * associated with. We also pass the access token as a Bearer token in the Authorization header.
   * @param context The context of the proxy request.
   * @return The headers to be passed to our implementation of the Kafka REST API.
   */
  @Override
  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    var headers = super.constructProxyHeaders(context);
    return headers
        .add(CONNECTION_ID_HEADER, String.valueOf(context.getConnectionId()))
        .add(HttpHeaders.AUTHORIZATION, "Bearer %s".formatted(accessToken.getToken()));
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
   * @param proxyResponse The response body from the Kafka REST API.
   * @param clusterUri The URI of the Kafka REST API running alongside the Kafka cluster.
   *                   (unused here)
   * @param sidecarHost The host of the sidecar.
   * @return The response body with the internal Kafka REST path replaced with the external path.
   */
  @Override
  public String processProxyResponse(String proxyResponse, String clusterUri, String sidecarHost) {
    return super
        .processProxyResponse(proxyResponse, sidecarHost, sidecarHost)
        .replaceAll("%s/internal/kafka".formatted(sidecarHost), "%s/kafka".formatted(sidecarHost));
  }
}
