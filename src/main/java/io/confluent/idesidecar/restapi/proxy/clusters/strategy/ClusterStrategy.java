package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Holds default implementations for processing constructing the proxy URI, headers before
 * forwarding the request to the cluster, and for processing the response from the cluster.
 *
 * @see ConfluentCloudKafkaClusterStrategy
 * @see ConfluentLocalKafkaClusterStrategy
 * @see ConfluentCloudSchemaRegistryClusterStrategy
 */
public abstract class ClusterStrategy {

  static UriUtil uriUtil = new UriUtil();

  @ConfigProperty(name = "ide-sidecar.cluster-proxy.http-header-exclusions")
  List<String> httpHeaderExclusions;

  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    var headers = HttpHeaders.headers();
    context.getRequestHeaders().forEach(header -> {
      if (!httpHeaderExclusions.contains(header.getKey())) {
        headers.add(header.getKey(), header.getValue());
      }
    });
    return headers;
  }

  public String constructProxyUri(String requestUri, String clusterUri) {
    return uriUtil.combine(clusterUri, requestUri);
  }

  /**
   * Process the proxy response by replacing the cluster URI with the sidecar URI. Accept sidecar
   * URI as a parameter to ease writing tests.
   */
  public String processProxyResponse(String proxyResponse,
      String clusterUri,
      String sidecarUri) {
    var clusterHost = uriUtil.getHost(clusterUri);
    String clusterPattern = "(http|https):\\/\\/(%s)(:\\d+)?".formatted(
        Pattern.quote(clusterHost));
    return proxyResponse.replaceAll(clusterPattern, sidecarUri);
  }
}
