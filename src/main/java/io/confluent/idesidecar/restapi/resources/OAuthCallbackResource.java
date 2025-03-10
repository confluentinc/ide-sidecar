package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.CCloudAuthenticationFailedException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.util.VsCodeExtensionUtil;
import io.quarkus.logging.Log;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.Objects;
import javax.net.ssl.SSLHandshakeException;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * The Confluent Cloud UI redirects users to this endpoint, the OAuth <code>redirect_uri</code>,
 * after a successful authentication.
 */
@ApplicationScoped
@Path("/gateway/v1/callback-vscode-docs")
public class OAuthCallbackResource {

  static final String CCLOUD_HOMEPAGE_URI = ConfigProvider.getConfig()
      .getOptionalValue("ide-sidecar.connections.ccloud.resources.homepage-uri", String.class)
      .orElse("https://confluent.cloud");

  // upon rendering the callback HTML, the user will be redirected to either their locally-running
  // VS Code instance (where the extension is installed, and the auth flow was initiated) or the
  // VS Code extension marketplace page for the extension.
  static final String CCLOUD_OAUTH_VSCODE_EXTENSION_URI = ConfigProvider.getConfig()
      .getOptionalValue("ide-sidecar.connections.ccloud.oauth.vscode-extension-uri", String.class)
      .orElse("https://marketplace.visualstudio.com/items?itemName=confluentinc.vscode-confluent");

  static final String TLS_HANDSHAKE_ERROR_MESSAGE =
      "Failed to perform the SSL/TLS handshake. Consider configuring custom certificates in the "
      + "extension settings of Confluent for VS Code if you are behind a firewall that performs "
      + "SSL inspection.";

  @Inject
  ConnectionStateManager mgr;

  @Inject
  VsCodeExtensionUtil vsCodeExtensionUtil;

  @Inject
  Template callback;

  @Inject
  @Location("callback_failure.html")
  Template callbackFailure;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Uni<String> callback(
      @QueryParam("code") String authorizationCode,
      @QueryParam("state") String oauthState
  ) {
    try {
      var connectionState = mgr.getConnectionStateByInternalId(oauthState);
      if (connectionState instanceof CCloudConnectionState cCloudConnectionState) {
        var redirectUri = vsCodeExtensionUtil.getRedirectUri(cCloudConnectionState);

        var response = cCloudConnectionState
            .getOauthContext()
            .createTokensFromAuthorizationCode(
                authorizationCode,
                cCloudConnectionState.getSpec().ccloudOrganizationId()
            )
            .map(authContext ->
                callback
                    .data("email", authContext.getUserEmail())
                    .data("confluent_cloud_homepage", CCLOUD_HOMEPAGE_URI)
                    .data("vscode_redirect_uri", redirectUri)
                    .render()
            )
            .recover((error) -> renderFailure(error, redirectUri))
            // Upon completion of the auth flow, refresh the status of the CCloud connection
            .compose(renderedTemplate ->
                cCloudConnectionState.refreshStatus().map(ignored -> renderedTemplate)
            );
        return Uni
            .createFrom()
            .completionStage(
                response.toCompletionStage()
            );
      } else {
        throw new CCloudAuthenticationFailedException(
            String.format(
                "Called callback page for non-CCloud connection (ID=%s).",
                connectionState.getId()
            )
        );
      }
    } catch (CCloudAuthenticationFailedException e) {
      return Uni
          .createFrom()
          .completionStage(
              renderFailure(e).toCompletionStage()
          );
    } catch (ConnectionNotFoundException e) {
      var failure = new ConnectionNotFoundException(
          "Invalid or expired state %s".formatted(oauthState)
      );
      return Uni
          .createFrom()
          .completionStage(
              renderFailure(failure).toCompletionStage()
          );
    }
  }

  private Future<String> renderFailure(Throwable error) {
    return renderFailure(error, null);
  }

  private Future<String> renderFailure(Throwable error, String redirectUri) {
    Log.error(error);
    var errorMessage = error instanceof SSLHandshakeException
        ? TLS_HANDSHAKE_ERROR_MESSAGE
        : error.getMessage();
    return Future.succeededFuture(
        callbackFailure
            .data("error", errorMessage)
            .data("vscode_redirect_uri", Objects.requireNonNullElse(redirectUri, ""))
            .render()
    );
  }
}
