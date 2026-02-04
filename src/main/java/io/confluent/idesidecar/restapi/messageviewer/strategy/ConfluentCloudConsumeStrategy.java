package io.confluent.idesidecar.restapi.messageviewer.strategy;

import static io.quarkus.arc.impl.UncaughtExceptions.LOGGER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecordHeader;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.RecordMetadata;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.idesidecar.restapi.proxy.ProxyHttpClient;
import io.confluent.idesidecar.restapi.util.ObjectMapperFactory;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Requests & handles the response from Confluent Cloud for message viewer functionality.
 */
@ApplicationScoped
public class ConfluentCloudConsumeStrategy implements ConsumeStrategy {

  static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();
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

  public Future<KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>> execute(
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context) {
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
    var proxyHttpClient = new ProxyHttpClient<KafkaRestProxyContext
        <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>
        >(webClientFactory, vertx);
    return proxyHttpClient
        .send(context)
        .compose(processedCtx -> vertx
            .createSharedWorkerExecutor("consume-worker")
            .executeBlocking(() -> postProcess(processedCtx))
        );
  }

  /**
   * Processes the MessageViewerContext by handling the response from Confluent Cloud.
   *
   * @param context The MessageViewerContext to process.
   * @return A Future containing the processed MessageViewerContext.
   */
  public KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> postProcess(
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context) {
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
      SimpleConsumeMultiPartitionResponse data = OBJECT_MAPPER.readValue(
          rawTopicRowsResponse,
          SimpleConsumeMultiPartitionResponse.class
      );
      var processedPartitionResponse = decodeSchemaEncodedValues(context, data);
      context.setResponse(processedPartitionResponse);
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
  private KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>
  handleEmptyOrNullResponseFromCCloud(
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context
  ) {
    var data = new SimpleConsumeMultiPartitionResponse(
        context.getClusterId(),
        context.getTopicName(),
        new ArrayList<>());
    context.setResponse(data);
    return context;
  }

  /**
   * Decodes schema-encoded values in the MultiPartitionConsumeResponse.
   *
   * @param context     The MessageViewerContext.
   * @param rawResponse The MultiPartitionConsumeResponse to decode.
   */
  private SimpleConsumeMultiPartitionResponse decodeSchemaEncodedValues(
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context,
      SimpleConsumeMultiPartitionResponse rawResponse
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

    return new SimpleConsumeMultiPartitionResponse(
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
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context,
      SchemaRegistryClient schemaRegistryClient
  ) {

    var processedRecords = partitionConsumeData
        .records().stream()
        .map(record -> {
          var decodedHeaders = decodeHeaders(record);
          var keyData = deserialize(
              record.key(),
              schemaRegistryClient,
              context,
              true,
              decodedHeaders
          );
          var valueData = deserialize(
              record.value(),
              schemaRegistryClient,
              context,
              false,
              decodedHeaders
          );

          return new PartitionConsumeRecord(
              record.partitionId(),
              record.offset(),
              record.timestamp(),
              record.timestampType(),
              toRecordHeaders(decodedHeaders),
              keyData.value(),
              valueData.value(),
              new RecordMetadata(
                  keyData.metadata(),
                  valueData.metadata()
              ),
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
   * Decodes the Base64 encoded header values from Confluent Cloud and converts the values to byte
   * arrays.
   *
   * @param partitionConsumeRecord The PartitionConsumeRecord containing the headers to decode.
   * @return The decoded Headers.
   */
  private Headers decodeHeaders(PartitionConsumeRecord partitionConsumeRecord) {
    var headers = new RecordHeaders();
    for (var header : partitionConsumeRecord.headers()) {
      byte[] decodedValue;
      try {
        decodedValue = BASE64_DECODER.decode(header.value());
      } catch (IllegalArgumentException e) {
        // For whatever reason, we couldn't decode the Base64 encoded header value.
        // Perhaps the API changed or the data is corrupted. Either way,
        // we log a debug and return the raw value as bytes.
        Log.debugf(e, "Failed to base64 decode header value '%s' from Confluent Cloud" +
                "(partition: %d, offset: %d)",
            header.value(),
            partitionConsumeRecord.partitionId(),
            partitionConsumeRecord.offset()
        );
        decodedValue = header.value() != null
            ? header.value().getBytes(StandardCharsets.UTF_8)
            : null;
      }
      headers.add(header.key(), decodedValue);
    }
    return headers;
  }

  /**
   * Confluent Cloud specific deserialization logic. We might get back a __raw__ field in the
   * message data, which indicates that the data is schema-encoded. If so, we first Base64 decode
   * the data and use the . If not, we return the message data as is.
   *
   * @param data                 The JsonNode containing the message data.
   * @param schemaRegistryClient The SchemaRegistryClient to use for decoding.
   * @param context              The message viewer context.
   * @return The decoded value.
   */
  private RecordDeserializer.DecodedResult deserialize(
      JsonNode data,
      SchemaRegistryClient schemaRegistryClient,
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context,
      boolean isKey,
      Headers headers
  ) {
    if (data.has("__raw__")) {
      // We know that Confluent Cloud encodes raw data in Base64, so decode appropriately.
      try {
        return deserializer.deserialize(
            BASE64_DECODER.decode(data.get("__raw__").asText()),
            schemaRegistryClient,
            context,
            isKey,
            // If deserialize fails, we want to return the raw data unchanged.
            Optional.of(BASE64_ENCODER::encode),
            headers
        );
      } catch (IllegalArgumentException e) {
        // For whatever reason, we couldn't decode the Base64 string.
        // Log the error and return the raw data.
        Log.error("Error decoding Base64 string: %s", data, e);
        return new RecordDeserializer.DecodedResult(data, e.getMessage());
      }
    } else {
      return deserializer.deserialize(
          data.asText().getBytes(StandardCharsets.UTF_8),
          schemaRegistryClient,
          context,
          isKey
      );
    }
  }

  /**
   * Constructs the URL to query messages from the specified topic in Confluent Cloud.
   *
   * @param ctx MessageViewerContext.
   * @return the constructed URL
   */
  protected String constructCCloudURL(
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> ctx) {
    // Replace with the actual URL after graphQL code is merged.
    final String hostName = ctx.getKafkaClusterInfo().uri();
    return hostName
        + "/kafka/v3/clusters/" + ctx.getClusterId()
        + "/internal/topics/" + ctx.getTopicName()
        + "/partitions/-/records:consume_guarantee_progress";
  }

  /**
   * Converts Kafka Headers to a list of PartitionConsumeRecordHeader. Converts header values
   * from byte arrays to strings.
   *
   * @param headers the Kafka Headers
   * @return a list of PartitionConsumeRecordHeader
   */
  private List<PartitionConsumeRecordHeader> toRecordHeaders(Headers headers) {
    var result = new ArrayList<PartitionConsumeRecordHeader>();
    for (var header : headers) {
      result.add(
          new PartitionConsumeRecordHeader(
            header.key(),
            header.value() != null ? new String(header.value(), StandardCharsets.UTF_8) : null
          )
      );
    }
    return result;
  }
}
