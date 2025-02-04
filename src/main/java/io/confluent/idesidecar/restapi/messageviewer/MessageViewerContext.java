package io.confluent.idesidecar.restapi.messageviewer;

import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.SchemaRegistry;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import java.util.Map;
import java.util.Optional;

/**
 * Stores the context of a request of the message viewer API.
 */
public class MessageViewerContext extends ClusterProxyContext {
  private final String topicName;
  private KafkaCluster kafkaClusterInfo;
  private SchemaRegistry schemaRegistryInfo;
  private final SimpleConsumeMultiPartitionRequest consumeRequest;

  private SimpleConsumeMultiPartitionResponse consumeResponse;

  public MessageViewerContext(
      String requestUri,
      MultiMap requestHeaders,
      HttpMethod requestMethod,
      SimpleConsumeMultiPartitionRequest requestBody,
      Map<String, String> requestPathParams,
      String connectionId,
      String clusterId,
      String topicName
  ) {
    super(
        requestUri,
        requestHeaders,
        requestMethod,
        Optional
            .ofNullable(requestBody)
            .map(body -> Buffer.buffer(body.toJsonString()))
            .orElse(null),
        requestPathParams,
        connectionId,
        clusterId,
        ClusterType.KAFKA
    );
    this.topicName = topicName;
    this.consumeRequest = requestBody;
  }

  public String getTopicName() {
    return this.topicName;
  }

  public void setKafkaClusterInfo(KafkaCluster info) {
    this.kafkaClusterInfo = info;
  }

  public KafkaCluster getKafkaClusterInfo() {
    return this.kafkaClusterInfo;
  }

  public void setSchemaRegistryInfo(SchemaRegistry info) {
    this.schemaRegistryInfo = info;
  }

  public SchemaRegistry getSchemaRegistryInfo() {
    return this.schemaRegistryInfo;
  }

  public SimpleConsumeMultiPartitionResponse getConsumeResponse() {
    return consumeResponse;
  }

  public void setConsumeResponse(
      SimpleConsumeMultiPartitionResponse consumeResponse) {
    this.consumeResponse = consumeResponse;
  }

  public SimpleConsumeMultiPartitionRequest getConsumeRequest() {
    return this.consumeRequest;
  }

  public boolean hasOverrideSR() {
    return getRequestHeaders() != null &&
        getRequestHeaders().contains(
            RequestHeadersConstants.MSG_VIEWER_OVERRIDE_SCHEMA_REGISTRY_CLUSTER_ID)
        && getRequestHeaders().contains(
            RequestHeadersConstants.MSG_VIEWER_OVERRIDE_SCHEMA_REGISTRY_CONNECTION_ID)
        // And that the override SR is not bogus but actually exists
        && getSchemaRegistryInfo() != null;
  }

  public Optional<Integer> maybeOverrideSchemaId() {
    if (hasOverrideSR()) {
      return Optional.of(consumeRequest.schemaId());
    } else {
      return Optional.empty();
    }
  }
}
