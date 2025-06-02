package io.confluent.idesidecar.restapi.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatchException;
import com.github.fge.jsonpatch.mergepatch.JsonMergePatch;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.CreateConnectionException;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionsList;
import io.quarkus.logging.Log;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;

/**
 * API endpoints for managing Sidecar connections. We use the {@link Blocking} annotation to run the
 * endpoints on the Quarkus worker thread pool instead of the event loop threads (also called I/O
 * threads). This is because we call the blocking method
 * {@link io.confluent.idesidecar.restapi.auth.CCloudOAuthContext#checkAuthenticationStatus} in some
 * of the endpoints.
 */
@Path(ConnectionsResource.API_RESOURCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Blocking
public class ConnectionsResource {

  public static final String API_RESOURCE_PATH = "/gateway/v1/connections";

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  Vertx vertx;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public ConnectionsList listConnections() {
    var connections = connectionStateManager
        .getConnectionStates()
        .stream()
        .map(connection -> getConnectionModel(connection.getSpec().id()))
        .toList();
    return new ConnectionsList(connections);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Uni<Connection> createConnection(
      ConnectionSpec connectionSpec,
      @Schema(
          description =
              "Whether to validate the connection spec and determine the connection status "
                  + "without creating the connection",
          defaultValue = "false"
      )
      @QueryParam("dry_run")
      boolean dryRun
  ) throws ConnectionNotFoundException, CreateConnectionException {
    if (dryRun) {
      // Just test the connection and return the status
      var testedState = connectionStateManager.testConnectionState(connectionSpec);
      // Get the status of the connection
      var futureStatus = testedState.refreshStatus();
      // And create a uni that will complete when the status is available
      return Uni
          .createFrom()
          .completionStage(futureStatus.toCompletionStage())
          .map(connectionStatus ->
              Connection.from(testedState, connectionStatus)
          );
    }
    // Create the connection
    var connection = connectionStateManager.createConnectionState(connectionSpec);
    // Immediately kick off async check of the new connection, independent of the periodic scheduled
    // task, which may not fire for "a while" from now. Overlapping checks at connection creation
    // time are fine and should resolve to the same state.
    vertx.executeBlocking(connection::refreshStatus);
    // Return the connection including the current/initial status. Websocket pushes will happen when
    // async checks update the status.
    return Uni
        .createFrom()
        .item(
            Connection.from(connection, connection.getStatus())
        );
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Connection getConnection(@PathParam("id") String id) {
    return getConnectionModel(id);
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
          description = "Could not authenticate",
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
        .chain(ignored -> {
          // Immediately kick off async check of the updated connection, independent of the periodic
          // scheduled task, which may not fire for "a while" from now.
          var connection = connectionStateManager.getConnectionState(id);
          vertx.executeBlocking(connection::refreshStatus);
          return Uni.createFrom().item(() -> getConnectionModel(id));
        });
  }

  @PATCH
  @Path("/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @APIResponses(value = {
      @APIResponse(
          responseCode = "200",
          description = "Connection updated with PATCH",
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
          description = "Could not authenticate",
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
  public Uni<Connection> patchConnection(
      @PathParam("id") String id,
      JsonMergePatch patch
  ) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      var connection = connectionStateManager.getConnectionState(id);
      // Convert connection spec to JsonNode
      var existingSpecNode = mapper.valueToTree(connection.getSpec());
      // Apply the patch to the existing spec
      var patchedSpecNode = patch.apply(existingSpecNode);
      // Convert patched spec back to ConnectionSpec
      var patchedSpec = mapper.treeToValue(patchedSpecNode, ConnectionSpec.class);
      return connectionStateManager
          .updateSpecForConnectionState(id, patchedSpec)
          .chain(ignored -> {
            // Immediately kick off async check of the patched connection, independent of the
            // periodic scheduled task, which may not fire for "a while" from now.
            vertx.executeBlocking(connection::refreshStatus);
            return Uni.createFrom().item(() -> getConnectionModel(id));
          });
    } catch (JsonPatchException | IOException e) {
      Log.errorf(
          "Failed to patch connection: %s, Connection ID: %s, Request: %s",
          e.getMessage(),
          id,
          patch
      );
      throw new WebApplicationException(
          "Failed to patch connection, please check the format of your request",
          Response.Status.BAD_REQUEST
      );
    }
  }

  @DELETE
  @Path("{id}")
  public void deleteConnection(@PathParam("id") String id) throws ConnectionNotFoundException {
    connectionStateManager.deleteConnectionState(id);
  }

  private Connection getConnectionModel(String id) {
    return Connection.from(
        connectionStateManager.getConnectionState(id)
    );
  }
}
