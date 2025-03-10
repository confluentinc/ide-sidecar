package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.util.VsCodeExtensionUtil;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

@ApplicationScoped
@Path("/")
public class PasswordResetCallbackResource {

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  VsCodeExtensionUtil vsCodeExtensionUtil;

  @Inject
  @Location("reset_password.html")
  Template resetPasswordTemplate;

  @Inject
  @Location("default.html")
  Template defaultTemplate;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Uni<String> passwordResetCallback(
      @QueryParam("email") String email,
      @QueryParam("success") Boolean success,
      @QueryParam("message") String message
  ) {
    var confluentCloudConnection = connectionStateManager.getConnectionStates().stream()
        .filter(CCloudConnectionState.class::isInstance)
        .map(CCloudConnectionState.class::cast)
        .findFirst()
        .orElse(null);
    var redirectUri = vsCodeExtensionUtil.getRedirectUri(confluentCloudConnection);
    if (Boolean.TRUE.equals(success) && email != null && message != null) {
      return Uni
          .createFrom()
          .item(
              resetPasswordTemplate
                .data("email", email)
                .data("vscode_redirect_uri", redirectUri)
                .render()
          );
    } else {
      return Uni
          .createFrom()
          .item(
              defaultTemplate
                  .data("vscode_redirect_uri", redirectUri)
                  .render()
          );
    }
  }
}
