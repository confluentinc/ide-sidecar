package io.confluent.idesidecar.restapi.auth;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.MdsConfig;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.client.HttpResponse;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Authentication context for MDS, using the MDS credentials to authenticate and get a token.
 */
public class MdsAuthContext implements AuthContext {

  private final WebClientFactory webClientFactory = WebClientFactory.getOrCreateFactory();
  private final MdsConfig mdsConfig;
  private final AtomicReference<Token> token = new AtomicReference<>();

  public MdsAuthContext(@NotNull MdsConfig mdsConfig) {
    this.mdsConfig = mdsConfig;
  }

  @Override
  public Future<Boolean> checkAuthenticationStatus() {
    var valid = isTokenValid(token.get());
    return Future.succeededFuture(valid);
  }

  @Override
  public Optional<Instant> expiresAt() {
    var token = this.token.get();
    return token != null ? Optional.of(token.expiresAt()) : Optional.empty();
  }

  /**
   * Always get the authentication headers directly from the credentials, rather than using
   * the cached token.
   *
   * @return the authentication-related header(s) to use for HTTP requests to the MDS
   */
  public MultiMap getAuthenticationHeaders() {
    var headers = HttpHeaders.headers();
    var creds = mdsConfig.credentials();
    if (creds != null) {
      creds.httpClientHeaders().ifPresent(map -> map.forEach(headers::add));
    }
    return headers;
  }

  public Future<AuthContext> maybeRefresh() {
    // Do not refresh if the token is still valid
    if (isTokenValid(token.get())) {
      return Future.succeededFuture(this);
    }
    return refresh(null);
  }

  @Override
  public Future<AuthContext> refresh(String unusedOrganizationId) {
    // Get headers for MDS from the credentials
    var headers = getAuthenticationHeaders();

    // And make a request to the MDS to authenticate and get a token
    var baseUrl = mdsConfig.uri();
    var url = "%s/security/1.0/authenticate".formatted(baseUrl);
    var requestedAt = Instant.now();
    return webClientFactory
        .getWebClient()
        .getAbs(url)
        .putHeaders(headers) // includes the Authorization header with credentials
        .putHeader(HttpHeaders.ACCEPT.toString(), "application/json")
        .send()
        .onSuccess(r -> handleTokenSuccessResponse(r, requestedAt))
        .onFailure(e -> handleTokenFailureResponse(e, url))
        .map(this);
  }

  @Override
  public void reset() {
    token.set(null);
  }

  /**
   * Extract the token from the response and update the cached token reference.
   *
   * @param response    the response from the authentication request
   * @param requestedAt the time when the request was made, used to compute the expiration
   */
  private void handleTokenSuccessResponse(HttpResponse<Buffer> response, Instant requestedAt) {
    var authResponse = response.bodyAsJson(AuthenticateResponse.class);
    var newToken = new Token(
        authResponse.authToken(),
        requestedAt.plusSeconds(authResponse.expiresInSeconds())
    );
    token.set(newToken);
  }

  private void handleTokenFailureResponse(Throwable error, String url) {
    Log.debugf("Failed to authenticate at %s: %s", url, error.getMessage(), error);
    token.set(null);
  }

  private boolean isTokenValid(Token token) {
    return token != null && token.expiresAt().isAfter(Instant.now());
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record AuthenticateResponse(
      @JsonProperty(value = "auth_token") String authToken,
      @JsonProperty(value = "token_type") String tokenType,
      @JsonProperty(value = "expires_in") int expiresInSeconds
  ) {
  }
}
