package io.confluent.idesidecar.restapi.proxy;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.SchemaRegistry;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import java.util.Map;
import java.util.Optional;

/**
 * Stores the context of a request of the message viewer API.
 */
public class KafkaRestProxyContext<T, U> extends ClusterProxyContext {

  private final String topicName;
  private KafkaCluster kafkaClusterInfo;
  private SchemaRegistry schemaRegistryInfo;
  private final T request;

  private U response;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  public KafkaRestProxyContext(
      String requestUri,
      MultiMap requestHeaders,
      HttpMethod requestMethod,
      T requestBody,
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
            .map(body -> Buffer.buffer(toJsonString(requestBody)))
            .orElse(null),
        requestPathParams,
        connectionId,
        clusterId,
        ClusterType.KAFKA
    );
    this.topicName = topicName;
    this.request = requestBody;
  }

  /**
   * Use this if the request is originated from within sidecar code and not being passed through as
   * is from a client.
   */
  public KafkaRestProxyContext(
      String connectionId,
      String clusterId,
      String topicName,
      T requestBody
  ) {
    super(
        null,
        null,
        null,
        Optional
            .ofNullable(requestBody)
            .map(body -> Buffer.buffer(toJsonString(requestBody)))
            .orElse(null),
        null,
        connectionId,
        clusterId,
        ClusterType.KAFKA
    );
    this.topicName = topicName;
    this.request = requestBody;
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

  public U getResponse() {
    return response;
  }

  public void setResponse(U response) {
    this.response = response;
  }

  public T getRequest() {
    return this.request;
  }

  public static String toJsonString(Object object) {
    try {
      return OBJECT_MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
