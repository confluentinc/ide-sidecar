package io.confluent.idesidecar.restapi.messageviewer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeResponse;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.SchemaRegistry;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import java.util.Map;

/**
 * Stores the context of a request of the message viewer API.
 */
public class MessageViewerContext extends ProxyContext {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final String clusterId;
  private final String topicName;
  private KafkaCluster kafkaClusterInfo;
  private SchemaRegistry schemaRegistryInfo;
  private final ConsumeRequest consumeRequest;

  private ConsumeResponse consumeResponse;

  public MessageViewerContext(
      String requestUri,
      MultiMap requestHeaders,
      HttpMethod requestMethod,
      ConsumeRequest requestBody,
      Map<String, String> requestPathParams,
      String connectionId,
      String clusterId,
      String topicName
  ) throws JsonProcessingException {
    super(
        requestUri,
        requestHeaders,
        requestMethod,
        Buffer.buffer(OBJECT_MAPPER.writeValueAsString(requestBody)),
        requestPathParams,
        connectionId
    );
    this.clusterId = clusterId;
    this.topicName = topicName;
    this.consumeRequest = requestBody;
  }

  public String getClusterId() {
    return this.clusterId;
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

  public ConsumeResponse getConsumeResponse() {
    return consumeResponse;
  }

  public void setConsumeResponse(
      ConsumeResponse consumeResponse) {
    this.consumeResponse = consumeResponse;
  }

  public ConsumeRequest getConsumeRequest() {
    return this.consumeRequest;
  }
}
