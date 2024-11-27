package io.confluent.idesidecar.restapi.proxy.clusters;

import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.graph.Cluster;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ClusterStrategy;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import java.util.Map;

/**
 * Shared context model for:
 * <ul>
 *    <li> Kafka REST proxy </li>
 *    <li> Schema Registry REST proxy </li>
 * </ul>
 */
public class ClusterProxyContext extends ProxyContext {

  final String clusterId;
  final ClusterType clusterType;
  Cluster clusterInfo;
  ClusterStrategy clusterStrategy;

  public ClusterProxyContext(
      String requestUri,
      MultiMap requestHeaders,
      HttpMethod requestMethod,
      Buffer requestBody,
      Map<String, String> requestPathParams,
      @Nullable String connectionId,
      String clusterId,
      ClusterType clusterType
  ) {
    super(requestUri, requestHeaders, requestMethod, requestBody, requestPathParams, connectionId);
    this.clusterId = clusterId;
    this.clusterType = clusterType;
  }

  public Cluster getClusterInfo() {
    return clusterInfo;
  }

  public void setClusterInfo(Cluster clusterInfo) {
    this.clusterInfo = clusterInfo;
  }

  public ClusterStrategy getClusterStrategy() {
    return clusterStrategy;
  }

  public void setClusterStrategy(ClusterStrategy clusterStrategy) {
    this.clusterStrategy = clusterStrategy;
  }

  public String getClusterId() {
    return clusterId;
  }

  public ClusterType getClusterType() {
    return clusterType;
  }
}
