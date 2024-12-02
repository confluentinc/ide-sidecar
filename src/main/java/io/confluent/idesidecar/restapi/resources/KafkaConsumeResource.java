package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.clients.SchemaErrors;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponseSchema;

/**
 * Endpoints of the message viewer API.
 */
@Path("/gateway/v1")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class KafkaConsumeResource {

  public static final String KAFKA_CONSUMED_BYTES_RESPONSE_HEADER =
      "Kafka-Multi-Partition-Consume-Bytes";

  @Inject
  @Named("messageViewerProcessor")
  Processor<MessageViewerContext, Future<MessageViewerContext>> messageViewerProcessor;

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Parameter(
      name = "x-connection-id",
      description = "Connection ID",
      required = true,
      in = ParameterIn.HEADER,
      schema = @Schema(type = SchemaType.STRING)
  )
  @APIResponseSchema(SimpleConsumeMultiPartitionResponse.class)
  @Path("/clusters/{cluster_id}/topics/{topic_name}/partitions/-/consume")
  @Blocking
  public Uni<Response> messageViewer(
      @Context RoutingContext routingContext,
      @PathParam("cluster_id") String clusterId,
      @PathParam("topic_name") String topicName,
      SimpleConsumeMultiPartitionRequest requestBody
  ) {
    return uniStage(() -> messageViewerProcessor
        .process(createMessageViewerContext(routingContext, clusterId, topicName, requestBody))
        .map(messageViewerContext -> {
          // Extract the number of bytes of the response body
          SimpleConsumeMultiPartitionResponse response = messageViewerContext.getConsumeResponse();
          ObjectMapper objectMapper = new ObjectMapper();
          String jsonResponse;
          try {
            jsonResponse = objectMapper.writeValueAsString(response);
          } catch (JsonProcessingException e) {
            Log.errorf("Error while converting response to JSON. Error: '%s'",
                e.getMessage());
            throw new RuntimeException("Error converting response to JSON", e);
          }
          long consumedBytes = jsonResponse.getBytes(StandardCharsets.UTF_8).length;

          // Manually build the response so that we can fill the header
          // Kafka-Multi-Partition-Consume-Bytes
          return Response
              .ok()
              .entity(messageViewerContext.getConsumeResponse())
              .header(KAFKA_CONSUMED_BYTES_RESPONSE_HEADER, consumedBytes)
              .build();
        }).toCompletionStage());
  }

  /**
   * Create a MessageViewerContext from the given parameters.
   */
  public static MessageViewerContext createMessageViewerContext(
      RoutingContext routingContext,
      String clusterId,
      String topicName,
      SimpleConsumeMultiPartitionRequest requestBody
  ) {
    return new MessageViewerContext(
        routingContext.request().uri(),
        routingContext.request().headers(),
        routingContext.request().method(),
        requestBody,
        routingContext.pathParams(),
        new SchemaErrors.ConnectionId(
            routingContext.request().getHeader(RequestHeadersConstants.CONNECTION_ID_HEADER)
        ),
        clusterId,
        topicName
    );
  }
}

