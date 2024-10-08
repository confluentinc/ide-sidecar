package io.confluent.idesidecar.restapi.kafkarest.filters;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.kafkarest.model.Error;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.Optional;

@Provider
@ApplicationScoped
public class ConnectionIdHeaderFilter implements ContainerRequestFilter {

  @Inject
  ConnectionStateManager manager;

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    var connectionId = Optional.ofNullable(
        requestContext.getHeaderString(CONNECTION_ID_HEADER)
    );

    if (requestContext.getUriInfo().getPath().startsWith("/internal/kafka/v3")) {
      if (connectionId.isEmpty()) {
        requestContext.abortWith(
            Response
                .status(Status.BAD_REQUEST)
                .entity(Error
                    .builder()
                    .errorCode(400)
                    .message("Missing required header: " + CONNECTION_ID_HEADER).build()
                ).build()
        );
      }
//      else {
//        // Check that the connectionId exists
//        try {
//          manager.getConnectionState(connectionId.get());
//        } catch (ConnectionNotFoundException e) {
//          requestContext.abortWith(
//              Response
//                  .status(Status.NOT_FOUND)
//                  .entity(Error
//                      .builder()
//                      .errorCode(404)
//                      .message("Connection not found: " + connectionId.get()).build()
//                  ).build()
//          );
//        }
//      }
    }
  }
}
