package io.confluent.idesidecar.restapi.messageviewer.strategy;

import static io.quarkus.arc.impl.UncaughtExceptions.LOGGER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.DecoderUtil;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.proxy.ProxyHttpClient;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Requests & handles the response from Confluent Cloud for message viewer functionality.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@ApplicationScoped
public class ConfluentCloudConsumeStrategy implements ConsumeStrategy {

  private static final int SR_CACHE_SIZE = 10;

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  @Inject
  Vertx vertx;

  public Future<MessageViewerContext> execute(MessageViewerContext context) {
    context.setProxyRequestMethod(HttpMethod.POST);
    context.setProxyRequestAbsoluteUrl(constructCCloudURL(context));
    var connectionState = (CCloudConnectionState) context.getConnectionState();
    context.setProxyRequestHeaders(connectionState
        .getOauthContext()
        .getDataPlaneAuthenticationHeaders()
        .add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
    );
    if (context.getRequestBody() != null) {
      context.setProxyRequestBody(context.getRequestBody());
    } else {
      context.setProxyRequestBody(Buffer.buffer("{}"));
    }
    ProxyHttpClient<MessageViewerContext> proxyHttpClient = new ProxyHttpClient<>(webClientFactory);
    return proxyHttpClient.send(context).compose(processedCtx ->
        vertx.executeBlocking(() -> postProcess(processedCtx))
    );
  }

  /**
   * Processes the MessageViewerContext by handling the response from Confluent Cloud.
   *
   * @param context The MessageViewerContext to process.
   * @return A Future containing the processed MessageViewerContext.
   */
  public MessageViewerContext postProcess(MessageViewerContext context) {
    if (context.getProxyResponseStatusCode() >= 300) {
      Log.errorf(
          "Error fetching the messages from ccloud: %s",
          context.getProxyResponseBody());
      throw new ProcessorFailedException(
          context.failf(
              context.getProxyResponseStatusCode(),
              "Error fetching the messages from ccloud: %s".formatted(
                  context.getProxyResponseBody())
          )
      );
    }
    final String rawTopicRowsResponse = context.getProxyResponseBody().toString();
    if (rawTopicRowsResponse == null || rawTopicRowsResponse.isEmpty()) {
      return handleEmptyOrNullResponseFromCCloud(context);
    }

    ObjectMapper objectMapper = new ObjectMapper();
    try {
      SimpleConsumeMultiPartitionResponse data = objectMapper.readValue(
          rawTopicRowsResponse,
          SimpleConsumeMultiPartitionResponse.class
      );
      decodeSchemaEncodedValues(context, data);
      context.setConsumeResponse(data);
      return context;
    } catch (JsonProcessingException e) {
      LOGGER.error("Error parsing the messages from ccloud : \n message ='"
          + rawTopicRowsResponse + "' \n Error: '" + e.getMessage() + "'");
      throw new ProcessorFailedException(
              context.failf(
                  500,
                  "We tried to consume records from the topic=\"%s\" (cluster_id=\"%s\") but "
                  + "observed the following error: %s.",
                  context.getTopicName(),
                  context.getClusterId(),
                  rawTopicRowsResponse)
          );
    }
  }

  /**
   * Handles the case when the response from Confluent Cloud is empty or null.
   *
   * @param context The MessageViewerContext.
   * @return A Future containing the processed MessageViewerContext.
   */
  private MessageViewerContext handleEmptyOrNullResponseFromCCloud(
      MessageViewerContext context
  ) {
    var data = new SimpleConsumeMultiPartitionResponse(
        context.getClusterId(),
        context.getTopicName(),
        new ArrayList<>());
    context.setConsumeResponse(data);
    return context;
  }

  /**
   * Decodes schema-encoded values in the MultiPartitionConsumeResponse.
   *
   * @param context The MessageViewerContext.
   * @param data    The MultiPartitionConsumeResponse to decode.
   */
  private void decodeSchemaEncodedValues(
      MessageViewerContext context,
      SimpleConsumeMultiPartitionResponse data
  ) {
    SchemaRegistryClient schemaRegistryClient = createSchemaRegistryClient(context);
    if (schemaRegistryClient != null) {
      for (var partitionData : data.partitionDataList()) {
        for (var record : partitionData.records()) {
          // Decode the key
          DecoderUtil.DecodedResult decodedKeyResult = decodeValue(
              record.key(),
              schemaRegistryClient,
              context.getTopicName()
          );

          // Decode the value
          DecoderUtil.DecodedResult decodedValueResult = decodeValue(
              record.value(),
              schemaRegistryClient,
              context.getTopicName()
          );

          updateRecord(
              partitionData,
              record,
              decodedKeyResult.getValue(),
              decodedValueResult.getValue(),
              decodedKeyResult.getErrorMessage(),
              decodedValueResult.getErrorMessage()
          );
        }
      }
    }
  }

  /**
   * Decodes a value using the SchemaRegistryClient if it is schema-encoded.
   *
   * @param value                The value to decode.
   * @param schemaRegistryClient The SchemaRegistryClient to use for decoding.
   * @param topicName  The name of the topic.
   * @return The decoded value.
   */
  private DecoderUtil.DecodedResult decodeValue(
      JsonNode value,
      SchemaRegistryClient schemaRegistryClient,
      String topicName) {
    if (value.has("__raw__")) {
      String rawValue = value.get("__raw__").asText();
      return DecoderUtil.decodeAndDeserialize(rawValue, schemaRegistryClient, topicName);
    }
    return new DecoderUtil.DecodedResult(value, null);
  }

  /**
   * Updates a PartitionConsumeRecord with the decoded key and value.
   *
   * @param partitionData The PartitionConsumeData containing the record to update.
   * @param record        The PartitionConsumeRecord to update.
   * @param decodedKey    The decoded key if decoding is successful else null
   * @param decodedValue  The decoded value if decoding is successful else null.
   * @param keyErrorMessage The error message for key decoding if it failed, else null.
   * @param valueErrorMessage The error message for value decoding if it failed, else null.
   */
  private void updateRecord(
      SimpleConsumeMultiPartitionResponse.PartitionConsumeData partitionData,
      SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord record,
      JsonNode decodedKey,
      JsonNode decodedValue,
      String keyErrorMessage,
      String valueErrorMessage
  ) {
    SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord updatedRecord =
        new SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord(
            record.partitionId(),
            record.offset(),
            record.timestamp(),
            record.timestampType(),
            record.headers(),
            decodedKey,
            decodedValue,
            keyErrorMessage,
            valueErrorMessage,
            record.exceededFields()
        );
    int recordIndex = partitionData.records().indexOf(record);
    partitionData.records().set(recordIndex, updatedRecord);
  }

  /**
   * Creates a SchemaRegistryClient instance based on the MessageViewerContext.
   *
   * @param context The MessageViewerContext.
   * @return The created SchemaRegistryClient instance, or null if SchemaRegistryInfo is not
   *         available.
   */
  private SchemaRegistryClient createSchemaRegistryClient(MessageViewerContext context) {
    if (context.getSchemaRegistryInfo() == null) {
      return null;
    }

    var extraHeaders = Map.of(
        RequestHeadersConstants.CONNECTION_ID_HEADER, context.getConnectionId(),
        RequestHeadersConstants.CLUSTER_ID_HEADER, context.getSchemaRegistryInfo().id(),
        HttpHeaders.AUTHORIZATION, "Bearer %s".formatted(accessTokenBean.getToken())
    );

    return new CachedSchemaRegistryClient(
        // We use the SR Rest Proxy provided by the sidecar itself
        Collections.singletonList(sidecarHost),
        SR_CACHE_SIZE,
        Arrays.asList(
            new ProtobufSchemaProvider(),
            new AvroSchemaProvider(),
            new JsonSchemaProvider()
        ),
        Collections.emptyMap(),
        extraHeaders
    );
  }


  /**
   * Constructs the URL to query messages from the specified topic in Confluent Cloud.
   *
   * @param ctx MessageViewerContext.
   * @return the constructed URL
   */
  protected String constructCCloudURL(MessageViewerContext ctx) {
    // Replace with the actual URL after graphQL code is merged.
    final String hostName = ctx.getKafkaClusterInfo().uri();
    return hostName
        + "/kafka/v3/clusters/" + ctx.getClusterId()
        + "/internal/topics/" + ctx.getTopicName()
        + "/partitions/-/records:consume_guarantee_progress";
  }
}