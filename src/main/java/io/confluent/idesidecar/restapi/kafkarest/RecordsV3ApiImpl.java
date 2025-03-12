package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;


@RequestScoped
@Path("/internal/kafka/v3/clusters/{cluster_id}/topics/{topic_name}/records")
public class RecordsV3ApiImpl {

  @Inject
  NativeProduceRecord nativeProducer;

  @POST
  @Consumes({"application/json"})
  @Produces({"application/json", "text/html"})
  public Uni<ProduceResponse> produceRecord(
      @HeaderParam(CONNECTION_ID_HEADER) String connectionId,
      @PathParam("cluster_id") String clusterId,
      @PathParam("topic_name") String topicName,
      @QueryParam("dry_run") @DefaultValue("false") boolean dryRun,
      @Valid ProduceRequest produceRequest
  ) {
    return nativeProducer.produce(
        connectionId,
        clusterId,
        topicName,
        dryRun,
        produceRequest
    );
  }
}
