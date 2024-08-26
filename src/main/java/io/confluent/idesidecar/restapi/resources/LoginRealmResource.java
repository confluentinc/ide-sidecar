package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.util.UriUtil;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import org.eclipse.microprofile.config.ConfigProvider;

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

    return Uni.createFrom().completionStage(loginRealm);
  }
}
