package io.confluent.idesidecar.restapi.messageviewer.strategy;

import static io.quarkus.arc.impl.UncaughtExceptions.LOGGER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.cache.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeResponse;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeResponse.PartitionConsumeData;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeResponse.PartitionConsumeRecord;
import io.confluent.idesidecar.restapi.proxy.ProxyHttpClient;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
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
import java.util.Base64;
import java.util.Optional;

/**
 * Requests & handles the response from Confluent Cloud for message viewer functionality.
 */
@ApplicationScoped
public class ConfluentCloudConsumeStrategy implements ConsumeStrategy {
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
  static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  RecordDeserializer deserializer;

  @Inject
  SchemaRegistryClients schemaRegistryClients;

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

    try {
      ConsumeResponse data = OBJECT_MAPPER.readValue(
          rawTopicRowsResponse,
          ConsumeResponse.class
      );
      var processedPartitionResponse = decodeSchemaEncodedValues(context, data);
      context.setConsumeResponse(processedPartitionResponse);
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
    var data = new ConsumeResponse(
        context.getClusterId(),
        context.getTopicName(),
        new ArrayList<>());
    context.setConsumeResponse(data);
    return context;
  }

  /**
   * Decodes schema-encoded values in the MultiPartitionConsumeResponse.
   *
   * @param context     The MessageViewerContext.
   * @param rawResponse The MultiPartitionConsumeResponse to decode.
   */
  private ConsumeResponse decodeSchemaEncodedValues(
      MessageViewerContext context,
      ConsumeResponse rawResponse
  ) {
    var schemaRegistry = context.getSchemaRegistryInfo();
    if (schemaRegistry == null) {
      return rawResponse;
    }

    var connectionId = context.getConnectionState().getId();
    var schemaRegistryClient = schemaRegistryClients.getClient(connectionId, schemaRegistry.id());
    if (schemaRegistryClient == null) {
      return rawResponse;
    }

    var processedPartitions = rawResponse
        .partitionDataList().stream()
        .map(partitionData -> processPartition(partitionData, context, schemaRegistryClient))
        .toList();

    return new ConsumeResponse(
        rawResponse.clusterId(),
        rawResponse.topicName(),
        processedPartitions
    );
  }

  /**
   * Decodes the keys and the values of all records of a specific partition.
   *
   * @param partitionConsumeData The unprocessed data of the partition.
   * @param context              The context of the message viewer API.
   * @param schemaRegistryClient The schema registry client to use for decoding the keys/values.
   * @return All records of the partition with decoded keys and values.
   */
  private PartitionConsumeData processPartition(
      PartitionConsumeData partitionConsumeData,
      MessageViewerContext context,
      SchemaRegistryClient schemaRegistryClient
  ) {

    var processedRecords = partitionConsumeData
        .records().stream()
        .map(record -> {
          var keyData = deserialize(
              record.key(),
              schemaRegistryClient,
              context.getTopicName(),
              true
          );
          var valueData = deserialize(
              record.value(),
              schemaRegistryClient,
              context.getTopicName(),
              false
          );

          return new PartitionConsumeRecord(
              record.partitionId(),
              record.offset(),
              record.timestamp(),
              record.timestampType(),
              record.headers(),
              keyData.value(),
              valueData.value(),
              keyData.errorMessage(),
              valueData.errorMessage(),
              record.exceededFields()
          );
        })
        .toList();

    return new PartitionConsumeData(
        partitionConsumeData.partitionId(),
        partitionConsumeData.nextOffset(),
        processedRecords
    );
  }

  /**
   * Confluent Cloud specific deserialization logic. We might get back a __raw__ field in the
   * message data, which indicates that the data is schema-encoded. If so, we first Base64 decode
   * the data and use the . If not, we return the message data as is.
   *
   * @param data                The value to decode.
   * @param schemaRegistryClient The SchemaRegistryClient to use for decoding.
   * @param topicName            The name of the topic.
   * @return The decoded value.
   */
  private RecordDeserializer.DecodedResult deserialize(
      JsonNode data,
      SchemaRegistryClient schemaRegistryClient,
      String topicName,
      boolean isKey
  ) {
    if (data.has("__raw__")) {
      // We know that Confluent Cloud encodes raw data in Base64, so decode appropriately.
      byte[] decodedBytes;
      try {
        decodedBytes = BASE64_DECODER.decode(data.get("__raw__").asText());
      } catch (IllegalArgumentException e) {
        // For whatever reason, we couldn't decode the Base64 string.
        // Log the error and return the raw data.
        Log.error("Error decoding Base64 string: %s", data, e);
        return new RecordDeserializer.DecodedResult(data, e.getMessage());
      }

      // Pass the decoded bytes to the deserializer to get the JSON representation
      // of the data, informed by the schema.
      return deserializer.deserialize(
          decodedBytes,
          schemaRegistryClient,
          topicName,
          isKey,
          Optional.of(BASE64_ENCODER::encode)
      );
    } else {
      // Data is not schema-encoded, so return it as is.
      return new RecordDeserializer.DecodedResult(data, null);
    }
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