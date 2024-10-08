package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import org.eclipse.microprofile.config.ConfigProvider;

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
  static final String sidecarHost = ConfigProvider.getConfig()
      .getValue("ide-sidecar.api.host", String.class);

  public MultiMap constructProxyHeaders(ClusterProxyContext context) {
    return HttpHeaders.headers();
  }

  public String constructProxyUri(String requestUri, String clusterUri) {
    return uriUtil.combine(clusterUri, requestUri);
  }

  /**
   * Process the proxy response from the cluster. Replace any HTTP URLs with the sidecar host.
   * We might run the risk of replacing URLs that are not pointing to the target cluster, but
   * we don't expect this to be a problem in the context of the Kafka REST API responses.
   */
  public String processProxyResponse(String proxyResponse) {
    var clusterPattern = "(http|https)://[\\w\\-.]+(:\\d+)?";
    return proxyResponse.replaceAll(clusterPattern, sidecarHost);
  }
}
