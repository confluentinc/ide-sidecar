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
import io.confluent.idesidecar.restapi.messageviewer.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord;
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

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  SchemaRegistryClients schemaRegistryClients;

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

    try {
      SimpleConsumeMultiPartitionResponse data = OBJECT_MAPPER.readValue(
          rawTopicRowsResponse,
          SimpleConsumeMultiPartitionResponse.class
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
   * @param context     The MessageViewerContext.
   * @param rawResponse The MultiPartitionConsumeResponse to decode.
   */
  private SimpleConsumeMultiPartitionResponse decodeSchemaEncodedValues(
      MessageViewerContext context,
      SimpleConsumeMultiPartitionResponse rawResponse
  ) {
    var schemaRegistry = context.getSchemaRegistryInfo();
    if (schemaRegistry == null) {
      return rawResponse;
    }

    var connectionId = context.getConnectionState().getId();
    var schemaRegistryClient = schemaRegistryClients.getCCloudClient(
        connectionId,
        schemaRegistry.id(),
        () -> createSchemaRegistryClient(context)
    );
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
      MessageViewerContext context,
      SchemaRegistryClient schemaRegistryClient
  ) {

    var processedRecords = partitionConsumeData
        .records().stream()
        .map(record -> {
          var decodedRecordKey = decodeValue(
              record.key(),
              schemaRegistryClient,
              context.getTopicName()
          );
          var decodedRecordValue = decodeValue(
              record.value(),
              schemaRegistryClient,
              context.getTopicName()
          );

          return new PartitionConsumeRecord(
              record.partitionId(),
              record.offset(),
              record.timestamp(),
              record.timestampType(),
              record.headers(),
              decodedRecordKey.getValue(),
              decodedRecordValue.getValue(),
              decodedRecordKey.getErrorMessage(),
              decodedRecordValue.getErrorMessage(),
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