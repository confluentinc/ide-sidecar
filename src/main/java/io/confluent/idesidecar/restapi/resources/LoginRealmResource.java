package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.util.ExceptionUtil;
import io.confluent.idesidecar.restapi.util.TimeUtil;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.netty.channel.ConnectTimeoutException;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.core.UriInfo;
import java.util.UUID;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Proxy endpoint called by the Confluent Cloud UI to determine the authentication mechanism (SSO or
 * username/password) used by the principal.
 */
@Path("/api/login/realm")
public class LoginRealmResource {

  private static final String CONFLUENT_CLOUD_LOGIN_REALM_URI = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.oauth.login-realm-uri", String.class);

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  UriUtil uriUtil;

  @GET
  public Uni<Response> proxyLoginRealmRequest(@Context UriInfo uriInfo) {
    var queryParameters = uriUtil.getFlattenedQueryParameters(uriInfo);

    var loginRealm = webClientFactory.getWebClient()
        .getAbs(CONFLUENT_CLOUD_LOGIN_REALM_URI.concat(queryParameters))
        .timeout(WebClientFactory.WEBCLIENT_REQUEST_TIMEOUT_MS)
        .send()
        .map(response ->
            Response
                // Proxy response status
                .status(response.statusCode())
                // Proxy response body
                .entity(response.body().toString())
                // Proxy response header `Content-Type`
                .header(HttpHeaders.CONTENT_TYPE, response.getHeader(HttpHeaders.CONTENT_TYPE))
                .build())
        .toCompletionStage();

    return Uni.createFrom().completionStage(loginRealm)
        .onFailure(ExceptionUtil::isTimeout)
        .recoverWithItem(timeout -> {
          Log.errorf(timeout, "Confluent Cloud login realm lookup timed out");
          var failure = new Failure(
              (Exception) timeout,
              Status.GATEWAY_TIMEOUT,
              "login_realm_timeout",
              ExceptionUtil.findCause(timeout, ConnectTimeoutException.class) != null
                  ? "Couldn't connect to Confluent Cloud in time"
                  : "Confluent Cloud didn't respond within %s".formatted(
                      TimeUtil.humanReadableDuration(
                          WebClientFactory.WEBCLIENT_REQUEST_TIMEOUT_SECONDS
                      )
                  ),
              UUID.randomUUID().toString(),
              null
          );
          return Response
              .status(Status.GATEWAY_TIMEOUT)
              .entity(failure)
              .type(MediaType.APPLICATION_JSON)
              .build();
        });
  }
}
