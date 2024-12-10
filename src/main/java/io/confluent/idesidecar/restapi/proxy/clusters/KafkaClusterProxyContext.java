package io.confluent.idesidecar.restapi.proxy.clusters;

import io.confluent.idesidecar.restapi.models.ClusterType;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import java.util.Map;

public class KafkaClusterProxyContext extends ClusterProxyContext {

  public KafkaClusterProxyContext(String requestUri, MultiMap requestHeaders,
      HttpMethod requestMethod, Buffer requestBody, Map<String, String> requestPathParams,
      @Nullable String connectionId, String clusterId, ClusterType clusterType) {
    super(requestUri, requestHeaders, requestMethod, requestBody, requestPathParams, connectionId,
        clusterId, clusterType);
  }
}
