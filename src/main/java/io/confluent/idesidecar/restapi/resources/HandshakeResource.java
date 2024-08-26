package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.exceptions.TokenAlreadyGeneratedException;
import io.confluent.idesidecar.restapi.models.SidecarAccessToken;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponseSchema;

/**
 * Single-use resource that returns an access token. If hit more than once, it will return a 401
 * Unauthorized. The single-use nature of this resource is enforced by the application-scoped
 * {@link SidecarAccessTokenBean}.
 */
@Path(HandshakeResource.API_RESOURCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class HandshakeResource {

  public static final String API_RESOURCE_PATH = "/gateway/v1/handshake";
  public static final String SIDECAR_PID_HEADER = "x-sidecar-pid";

  @Inject
  private SidecarAccessTokenBean accessTokenBean;

  @GET
  @APIResponseSchema(value = SidecarAccessToken.class, responseCode = "200")
  public Response handshake() {

    try {
      // Will work exactly once.
      var token = accessTokenBean.generateToken();
      return Response.ok(new SidecarAccessToken(token)).build();
    } catch (TokenAlreadyGeneratedException e) {

      // The route must have been hit once already.
      // 401 Unauthorized, include my own process ID as response header.
      // add header to response so that the out-of-synch extension may kill us and start
      // a new one.

      return Response
          .status(Response.Status.UNAUTHORIZED)
          .header(SIDECAR_PID_HEADER, String.valueOf(ProcessHandle.current().pid()))
          .build();
    }
  }
}
