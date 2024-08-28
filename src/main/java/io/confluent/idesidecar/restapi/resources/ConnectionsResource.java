package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.CreateConnectionException;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionsList;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;

/**
 * API endpoints for managing Sidecar connections.
 */
@Path(ConnectionsResource.API_RESOURCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectionsResource {

  public static final String API_RESOURCE_PATH = "/gateway/v1/connections";

  @Inject
  ConnectionStateManager connectionStateManager;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<ConnectionsList> listConnections() {
    var connections = connectionStateManager
        .getConnectionStates()
        .stream()
        .map(connection -> getConnectionModel(connection.getSpec().id()))
        .toList();

    Future<ConnectionsList> future = Future.future(handler ->
        Future
            .join(connections)
            .onSuccess(result -> handler.complete(ConnectionsList.from(result.list())))
            .onFailure(handler::fail)
    );

    return Uni.createFrom().completionStage(future.toCompletionStage());
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Connection createConnection(ConnectionSpec connectionSpec)
      throws ConnectionNotFoundException, CreateConnectionException {

    var newSpec = connectionStateManager.createConnectionState(connectionSpec);
    return Connection.from(newSpec);
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Connection> getConnection(@PathParam("id") String id) {
    return Uni
        .createFrom()
        .completionStage(getConnectionModel(id).toCompletionStage());
  }

  @PUT
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @APIResponses(value = {
    @APIResponse(
        responseCode = "200",
        description = "Connection updated",
        content = {
          @Content(mediaType = "application/json",
              schema = @Schema(implementation = Connection.class))
        }),
    @APIResponse(
        responseCode = "404",
        description = "Connection not found",
        content = {
            @Content(mediaType = "application/json",
                schema = @Schema(implementation = Failure.class))
        }),
    @APIResponse(
        responseCode = "401",
        description = "Could not authenticate with updated connection configuration",
        content = {
            @Content(mediaType = "application/json",
                schema = @Schema(implementation = Failure.class))
        }),
    @APIResponse(
        responseCode = "400",
        description = "Invalid input",
        content = {
            @Content(mediaType = "application/json",
                schema = @Schema(implementation = Failure.class))
        }),
  })
  public Uni<Connection> updateConnection(@PathParam("id") String id, ConnectionSpec spec) {
    return connectionStateManager
        .updateSpecForConnectionState(id, spec)
        .chain(ignored ->
            Uni.createFrom().completionStage(getConnectionModel(id).toCompletionStage()));
  }

  @DELETE
  @Path("{id}")
  public void deleteConnection(@PathParam("id") String id) throws ConnectionNotFoundException {
    connectionStateManager.deleteConnectionState(id);
  }

  private Future<Connection> getConnectionModel(String id) {
    try {
      ConnectionState connectionState = connectionStateManager.getConnectionState(id);

      return connectionState
          .getConnectionStatus()
          .map(connectionStatus -> Connection.from(connectionState, connectionStatus));
    } catch (ConnectionNotFoundException e) {
      return Future.failedFuture(e);
    }
  }
}
