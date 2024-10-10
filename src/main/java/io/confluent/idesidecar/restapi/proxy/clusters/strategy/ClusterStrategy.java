package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;

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

  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    return HttpHeaders.headers();
  }

  public String constructProxyUri(String requestUri, String clusterUri) {
    return uriUtil.combine(clusterUri, requestUri);
  }

  /**
   * Process the proxy response from the cluster. By default, this method does nothing.
   */
  public String processProxyResponse(String proxyResponse) {
    return proxyResponse;
  }
}
