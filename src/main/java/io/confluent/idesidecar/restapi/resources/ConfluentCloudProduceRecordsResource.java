package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.kafkarest.ConfluentCloudProduceRecord;
import io.confluent.idesidecar.restapi.kafkarest.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;

/**
 * Endpoints of the message viewer API.
 */
@Path("/gateway/v1")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class ConfluentCloudProduceRecordsResource {

  @Inject
  ConfluentCloudProduceRecord ccloudProducer;

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Parameter(
      name = "x-connection-id",
      description = "Connection ID",
      required = true,
      in = ParameterIn.HEADER,
      schema = @Schema(type = SchemaType.STRING)
  )
  @Path("/clusters/{cluster_id}/topics/{topic_name}/records")
  @Blocking
  public Uni<ProduceResponse> produceRecord(
      @HeaderParam(CONNECTION_ID_HEADER) String connectionId,
      @PathParam("cluster_id") String clusterId,
      @PathParam("topic_name") String topicName,
      @QueryParam("dry_run") @DefaultValue("false") boolean dryRun,
      @RequestBody ProduceRequest produceRequest
  ) {
    return ccloudProducer.produce(
        connectionId,
        clusterId,
        topicName,
        dryRun,
        produceRequest
    );
  }
}
