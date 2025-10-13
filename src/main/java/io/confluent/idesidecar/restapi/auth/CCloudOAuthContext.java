package io.confluent.idesidecar.restapi.auth;

import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.exceptions.CCloudAuthenticationFailedException;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.util.CCloud;
import io.confluent.idesidecar.restapi.util.ObjectMapperFactory;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkus.arc.Arc;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.net.HttpCookie;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.Base64;

/**
 * <p>Implementation of an OAuth-based authentication flow with the API of Confluent Cloud. This
 * class manages the refresh token, the control plane token, and the data plane token. It can
 * refresh the tokens before they expire. The refreshing of the tokens must be invoked by the
 * caller.</p>
 *
 * <p>This class can be configured via several configuration options available under
 * <i>ide-sidecar.connections.ccloud</i>.</p>
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class CCloudOAuthContext implements AuthContext {

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();
  private static final String HASH_ALGORITHM = "SHA-256";
  private static final int CODE_VERIFIER_LENGTH = 32;
  private static final int OAUTH_STATE_PARAMETER_LENGTH = 32;
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final String UNKNOWN_EMAIL_PLACEHOLDER = "UNKNOWN";

  private final String codeChallenge;
  private final String codeVerifier;
  private final String oauthState;
  private final AtomicReference<Tokens> tokens = new AtomicReference<>(new Tokens());
  private final UriUtil uriUtil = new UriUtil();
  private final WebClientFactory webClientFactory;
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final WriteLock writeLock = readWriteLock.writeLock();
  private final ReadLock readLock = readWriteLock.readLock();

  /**
   * Initializes an instance of the CCloudOAuthContext with a unique OAuth state parameter, code
   * challenge, and code verifier. The state parameter and the code verifier are filled with 32
   * random bytes. The code challenge is set to the hash of the code verifier; for hashing, we make
   * use of the algorithm SHA-256.
   */
  public CCloudOAuthContext() {
    oauthState = createRandomEncodedString(OAUTH_STATE_PARAMETER_LENGTH);
    codeVerifier = createRandomEncodedString(CODE_VERIFIER_LENGTH);
    codeChallenge = createCodeChallenge();

    // If the ArC container is available, select the application-scoped instance of the
    // `WebClientFactory` so that we can re-use it; otherwise, create a new instance without
    // dependency injection
    var container = Arc.container();
    webClientFactory = (container != null)
        ? container.select(WebClientFactory.class).get()
        : new WebClientFactory();
  }

  /**
   * Performs an API call against CCloud's <code>/api/check_jwt</code> endpoint to see if the
   * control plane token is valid. Note that this method may block the calling thread on the
   * following conditions:
   * <ul>
   *   <li>When another task holds the write lock</li>
   *   <li>When the host machine is unable to resolve the CCloud server's hostname
   *       due to DNS issues. The {@link HttpRequest#send()} method synchronously tries to
   *       resolve the hostname before sending the request.
   *   </li>
   * </ul>
   *
   * @return Succeeded future holding the boolean value true if the token is valid. Succeeded future
   * holding the boolean value false if the token is not valid. Failed future holding the cause of
   * the failure if any error occurred while interacting with the CCloud API, e.g., the CCloud API
   * returned invalid JSON.
   */
  @Override
  public Future<Boolean> checkAuthenticationStatus() {
    writeLock.lock();
    try {
      final var controlPlaneToken = tokens.get().controlPlaneToken;
      // This context can't be authenticated if it does not hold a control plane token
      if (isTokenMissing(controlPlaneToken)) {
        var errorMessage =
            "Cannot verify authentication status because no control plane token is available. It's "
                + "likely that this connection has not yet completed the authentication with CCloud.";
        tokens.updateAndGet(oldTokens ->
            oldTokens.withErrors(
                oldTokens.errors.withAuthStatusCheck(errorMessage)));
        return Future.failedFuture(new CCloudAuthenticationFailedException(errorMessage));
      }

      return webClientFactory.getWebClient()
          .getAbs(CCloudOAuthConfig.CCLOUD_CONTROL_PLANE_CHECK_JWT_URI)
          .putHeaders(getControlPlaneAuthenticationHeaders())
          // Synchronously tries to DNS resolve the hostname before sending the request
          // Calling threads beware!
          .send()
          .map(result -> {
            try {
              var response = OBJECT_MAPPER.readValue(result.bodyAsString(), CheckJwtResponse.class);
              if (response.error() != null && !response.error().isNull()) {
                Log.errorf("Error in CCloud response while verifying the auth status "
                    + "of this connection: %s", response.error());
                // depending on what kinds of errors we get from CCloud, we might want to
                // throw a different exception here (e.g. 429 Too Many Requests)
              }
              // If the response does not return any error, we can assume that we are successfully
              // authenticated with the CCloud API.
              return response.error().isNull();
            } catch (JsonProcessingException e) {
              throw new CCloudAuthenticationFailedException(
                  "Could not parse the response from Confluent Cloud when verifying the "
                      + "authentication status of this connection.", e);
            }
          })
          .onSuccess(result ->
              // Reset any existing error related to auth status check
              tokens.updateAndGet(oldTokens ->
                  oldTokens.withErrors(
                      oldTokens.errors.withoutAuthStatusCheck()))
          )
          .onFailure(failure ->
              tokens.updateAndGet(oldTokens ->
                  oldTokens.withErrors(
                      oldTokens.errors.withAuthStatusCheck(failure.getMessage())))
          );
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * This method is called by the {@link RefreshCCloudTokensBean} to find out the earliest expiry
   * time of the tokens (refresh token, control plane token, and data plane token). It returns the
   * earliest expiry time to make sure that another refresh of all tokens is invoked if a refresh of
   * the tokens is only partially successful.
   */
  @Override
  public Optional<Instant> expiresAt() {
    readLock.lock();
    try {
      return tokens.get().expiresAt();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * This method can be used to refresh all three tokens in one go: refresh token, control plane
   * token, and data plane token. It (1) exchanges the current refresh token for an ID token, (2)
   * exchanges the ID token for a control plane token, and (3) exchanges the control plane token for
   * a data plane token. When exchanging the refresh for the ID token, we implicitly invalidate the
   * existing refresh token but get and persist a new one.
   *
   * @param organizationId The CCloud organization ID to use for the token exchange, if null, CCloud
   *                       will provide tokens for the default organization.
   * @return If successful, a succeeded future holding this auth context with up-to-date refresh,
   * control plane, and data plane tokens. If not successful, a failed future holding the cause of
   * the failure.
   */
  @Override
  public Future<AuthContext> refresh(String organizationId) {
    writeLock.lock();
    try {
      return bareRefresh(organizationId)
          // Reset any existing error related to token refresh flow
          .onSuccess(onSuccessfulTokenRefresh())
          // Persist error related to token refresh flow
          .onFailure(onFailedTokenRefresh());
    } finally {
      writeLock.unlock();
    }
  }

  public Future<AuthContext> refreshIgnoreFailures(String organizationId) {
    writeLock.lock();
    try {
      return bareRefresh(organizationId)
          .onSuccess(onSuccessfulTokenRefresh());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Voids all tokens and errors and resets this authentication context to its initial state.
   */
  @Override
  public void reset() {
    writeLock.lock();
    try {
      tokens.updateAndGet(oldTokens -> new Tokens());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * This method generates the unique sign-in URI for this authentication context, which includes
   * information, such as the state and the code challenge. Later on, you must use the same instance
   * of the CCloudOAuthContext for creating tokens because the code challenge (set in the sign-in
   * URI) and the code verifier (used in the method createTokensFromAuthorizationCode) are related
   * to each other.
   *
   * @return The URI to use for signing in
   */
  public String getSignInUri() {
    return String.join(
        "",
        List.of(
            CCloudOAuthConfig.CCLOUD_OAUTH_AUTHORIZE_URI,
            "?response_type=code",
            "&code_challenge_method=S256",
            "&code_challenge=", codeChallenge,
            "&state=", oauthState,
            "&client_id=", CCloudOAuthConfig.CCLOUD_OAUTH_CLIENT_ID,
            "&redirect_uri=", CCloudOAuthConfig.CCLOUD_OAUTH_REDIRECT_URI,
            "&scope=", uriUtil.encodeUri(
                CCloudOAuthConfig.CCLOUD_OAUTH_SCOPE)
        )
    );
  }

  /**
   * This method can be used to instantiate a fully-authenticated CCloudOAuthContext from the
   * authorization code passed from Confluent Cloud to the <code>redirect_uri</code> exposed by the
   * sidecar.
   *
   * @param authorizationCode    The authorization code passed from Confluent Cloud
   * @param ccloudOrganizationId The Confluent Cloud organization id
   * @return If successful, a succeeded future holding this auth context with up-to-date refresh,
   * control plane, and data plane tokens. If not successful, a failed future holding the cause of
   * the failure.
   */
  public Future<CCloudOAuthContext> createTokensFromAuthorizationCode(
      String authorizationCode,
      String ccloudOrganizationId
  ) {
    writeLock.lock();
    try {
      return exchangeAuthorizationCode(authorizationCode)
          .compose(response -> this.processTokenExchangeResponse(response, ccloudOrganizationId))
          .map(CCloudOAuthContext.class::cast)
          .onSuccess(result -> {
            // Reset any existing errors
            tokens.updateAndGet(oldTokens ->
                oldTokens.withErrors(
                    oldTokens.errors
                        .withoutSignIn()
                        .withoutTokenRefresh()
                        .withoutAuthStatusCheck()
                )
            );
            Log.infof(
                "User has successfully authenticated with CCloud (email=%s,organization_id=%s).",
                getUserEmail(), ccloudOrganizationId);
          })
          .onFailure(failure ->
              tokens.updateAndGet(oldTokens ->
                  oldTokens.withErrors(
                      oldTokens.errors.withSignIn(failure.getMessage())))
          );
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Check if we should attempt a token refresh. We will attempt a token refresh iff: (1) The
   * refresh token is valid and has not yet reached its absolute lifetime, (2) we have not yet
   * attempted more than
   * <code>ide-sidecar.connections.ccloud.refresh-token.max-refresh-attempts</code>
   * token refresh attempts, and (3) at least one token of the connection will expire before the
   * next run of this job.
   *
   * @return if we should attempt the token refresh.
   */
  public boolean shouldAttemptTokenRefresh() {
    readLock.lock();
    try {
      // Perform token refresh only if auth context will expire before next run of this job
      var now = Instant.now();
      var expiresAt = expiresAt();
      var nextExecution = now.plus(CCloudOAuthConfig.TOKEN_REFRESH_INTERVAL_SECONDS);
      var atLeastOneTokenWillExpireBeforeNextRun = expiresAt.isPresent()
          && expiresAt.get().compareTo(nextExecution) < 0;

      return !hasReachedEndOfLifetime()
          && !hasNonTransientError()
          && atLeastOneTokenWillExpireBeforeNextRun;
    } finally {
      readLock.unlock();
    }
  }

  public Token getRefreshToken() {
    return tokens.get().refreshToken;
  }

  public Token getControlPlaneToken() {
    return tokens.get().controlPlaneToken;
  }

  public Token getDataPlaneToken() {
    return tokens.get().dataPlaneToken;
  }

  public Instant getEndOfLifetime() {
    return tokens.get().endOfLifetime;
  }

  /**
   * Checks if the refresh token has reached the end of its absolute lifetime and requires a
   * re-authentication with Confluent Cloud.
   *
   * @return true if the end of the absolute lifetime is reached, false otherwise
   */
  public boolean hasReachedEndOfLifetime() {
    var endOfLifetime = getEndOfLifetime();
    return endOfLifetime != null && Instant.now().compareTo(endOfLifetime) >= 0;
  }

  public UserDetails getUser() {
    return tokens.get().user;
  }

  public OrganizationDetails getCurrentOrganization() {
    return tokens.get().organization;
  }

  public AuthErrors getErrors() {
    return tokens.get().errors;
  }

  public boolean hasNonTransientError() {
    return tokens.get().errors.hasNonTransientErrors();
  }

  public MultiMap getControlPlaneAuthenticationHeaders() {
    return getAuthenticationHeaders(tokens.get().controlPlaneToken);
  }

  public MultiMap getDataPlaneAuthenticationHeaders() {
    return getAuthenticationHeaders(tokens.get().dataPlaneToken);
  }

  public String getUserEmail() {
    var userDetails = getUser();
    return (userDetails != null) ? userDetails.email() : UNKNOWN_EMAIL_PLACEHOLDER;
  }

  public String getOauthState() {
    return oauthState;
  }

  public Integer getFailedTokenRefreshAttempts() {
    return tokens.get().failedTokenRefreshAttempts;
  }

  private Handler<Throwable> onFailedTokenRefresh() {
    return failure ->
        tokens.updateAndGet(oldTokens -> oldTokens.withFailedTokenRefreshAttempt(failure));
  }

  private Handler<AuthContext> onSuccessfulTokenRefresh() {
    return result -> tokens.updateAndGet(Tokens::withSuccessfulTokenRefreshAttempt);
  }

  private Future<AuthContext> bareRefresh(String organizationId) {
    return createTokensFromRefreshToken()
        .compose(
            idTokenExchangeResponse ->
                processTokenExchangeResponse(idTokenExchangeResponse, organizationId));
  }

  private MultiMap getAuthenticationHeaders(
      Token bearerToken) {
    var headers = MultiMap.caseInsensitiveMultiMap();
    if (bearerToken != null) {
      headers.add(AUTHORIZATION, "Bearer %s".formatted(bearerToken.token()));
    }
    return headers;
  }

  private Future<IdTokenExchangeResponse> exchangeAuthorizationCode(String authorizationCode) {
    var requestBody = String.format(
        "grant_type=authorization_code&client_id=%s&code=%s&code_verifier=%s&redirect_uri=%s",
        CCloudOAuthConfig.CCLOUD_OAUTH_CLIENT_ID,
        authorizationCode,
        codeVerifier,
        CCloudOAuthConfig.CCLOUD_OAUTH_REDIRECT_URI
    );

    // Reset the end of the lifetime of this authentication context, after which we can no longer
    // refresh tokens without users re-authenticating with CCloud
    tokens.updateAndGet(oldTokens ->
        oldTokens.withEndOfLifetime(
            Instant.now().plus(CCloudOAuthConfig.CCLOUD_REFRESH_TOKEN_ABSOLUTE_LIFETIME)));

    return performTokenExchange(requestBody);
  }

  private Future<IdTokenExchangeResponse> createTokensFromRefreshToken() {
    final var refreshToken = tokens.get().refreshToken;
    if (isTokenMissing(refreshToken)) {
      return Future.failedFuture(
          new CCloudAuthenticationFailedException("Refresh token is missing."));
    }
    var requestBody = String.format(
        "grant_type=refresh_token&client_id=%s&refresh_token=%s&redirect_uri=%s",
        CCloudOAuthConfig.CCLOUD_OAUTH_CLIENT_ID,
        refreshToken.token(),
        CCloudOAuthConfig.CCLOUD_OAUTH_REDIRECT_URI
    );

    return performTokenExchange(requestBody);
  }

  private Future<IdTokenExchangeResponse> performTokenExchange(String requestBody) {
    return webClientFactory.getWebClient()
        .postAbs(CCloudOAuthConfig.CCLOUD_OAUTH_TOKEN_URI)
        .putHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED)
        .sendBuffer(Buffer.buffer(requestBody))
        .map(response -> {
          try {
            return OBJECT_MAPPER.readValue(response.bodyAsString(), IdTokenExchangeResponse.class);
          } catch (JsonProcessingException e) {
            throw new CCloudAuthenticationFailedException(
                "Could not parse the response from Confluent Cloud when retrieving the ID token.",
                e);
          }
        });
  }

  /**
   * This method accepts the response of CCloud's /oauth/token endpoint, which includes the ID
   * token, exchanges the ID token for a control plane token, and exchanges the control plane for a
   * data plane token. If any of these exchanges fail, this method returns a failed Future holding
   * the cause of the failure.
   *
   * @param idTokenExchangeResponse - The response of CCloud's /oauth/token endpoint holding the ID
   *                                token
   * @param ccloudOrganizationId    The CCloud organization ID to use for the token exchange
   * @return If successful, a succeeded future holding this auth context with up-to-date refresh,
   * control plane, and data plane tokens. If not successful, a failed future holding the cause of
   * the failure.
   */
  private Future<AuthContext> processTokenExchangeResponse(
      IdTokenExchangeResponse idTokenExchangeResponse,
      String ccloudOrganizationId
  ) {

    if (idTokenExchangeResponse.error() != null) {
      throw new CCloudAuthenticationFailedException(
          String.format("Retrieving the ID token failed for the following reason: %s - %s",
              idTokenExchangeResponse.error().asText(),
              idTokenExchangeResponse.errorDescription().asText()));
    }

    final var now = Instant.now();
    tokens.updateAndGet(oldTokens ->
        oldTokens.withRefreshToken(
            new Token(
                idTokenExchangeResponse.refreshToken(),
                now.plusSeconds(idTokenExchangeResponse.expiresIn())
            )
        ));

    var request = new ExchangeControlPlaneTokenRequest(
        idTokenExchangeResponse.idToken(), ccloudOrganizationId);
    return exchangeControlPlaneToken(request)
        .compose(sessionTokenResult -> {
          if (sessionTokenResult.error() != null && !sessionTokenResult.error().isNull()) {
            throw new CCloudAuthenticationFailedException(
                "Retrieving the control plane token failed for the following reason: %s".formatted(
                    sessionTokenResult.error()));
          }
          tokens.updateAndGet(oldTokens ->
              oldTokens
                  .withControlPlaneToken(
                      new Token(
                          sessionTokenResult.token(),
                          now.plus(CCloudOAuthConfig.CCLOUD_CONTROL_PLANE_TOKEN_LIFE_TIME_SEC)
                      )
                  )
                  .withUser(sessionTokenResult.user)
                  .withOrganization(sessionTokenResult.organization));

          return exchangeDataPlaneToken(sessionTokenResult.token());
        }).compose(dataPlaneTokenExchangeResponseResult -> {
          if (dataPlaneTokenExchangeResponseResult.error() != null
              && !dataPlaneTokenExchangeResponseResult.error().isNull()) {
            throw new CCloudAuthenticationFailedException(
                "Retrieving the data plane token for the account %s failed: %s".formatted(
                    getUserEmail(),
                    dataPlaneTokenExchangeResponseResult.error()));
          }

          tokens.updateAndGet(oldTokens ->
              oldTokens.withDataPlaneToken(
                  new Token(
                      dataPlaneTokenExchangeResponseResult.token(),
                      now.plus(CCloudOAuthConfig.CCLOUD_CONTROL_PLANE_TOKEN_LIFE_TIME_SEC)
                  )
              ));

          return Future.succeededFuture(this);
        });
  }

  public Future<ControlPlaneTokenExchangeResponse> exchangeControlPlaneToken(
      ExchangeControlPlaneTokenRequest request) {
    return webClientFactory.getWebClient()
        .postAbs(CCloudOAuthConfig.CCLOUD_CONTROL_PLANE_TOKEN_URI)
        .putHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .sendBuffer(Buffer.buffer(request.toJsonString()))
        .map(response -> {
          try {
            ControlPlaneTokenExchangeResponse responseBody =
                OBJECT_MAPPER.readValue(response.bodyAsString(), ControlPlaneTokenExchangeResponse.class);

            String setCookieHeader = response.getHeader("Set-Cookie");
            String authToken = null;
            if (setCookieHeader != null) {
              List<HttpCookie> cookies = HttpCookie.parse(setCookieHeader);
              if (cookies.isEmpty()) {
                throw new CCloudAuthenticationFailedException(
                    "Set-Cookie header not found in response from Confluent Cloud: Set-Cookie header present but no cookies parsed.");
              }
              authToken = cookies.stream()
                  .filter(cookie -> cookie.getName().equals("auth_token"))
                  .findFirst()
                  .orElseThrow(() ->
                      new CCloudAuthenticationFailedException(
                          "Set-Cookie header found in response from Confluent Cloud."
                      )
                  )
                  .getValue();
            } else {
              throw new CCloudAuthenticationFailedException(
                  "Set-Cookie header not found in response from Confluent Cloud.");
            }

            return new ControlPlaneTokenExchangeResponse(
                authToken,
                responseBody.error,
                responseBody.user,
                responseBody.organization,
                responseBody.refreshToken(),
                responseBody.identityProvider()
            );
          } catch (JsonProcessingException e) {
            throw new CCloudAuthenticationFailedException(
                "Could not parse the response from Confluent Cloud when retrieving the control plane token.", e);
          }
        });
  }

  private Future<DataPlaneTokenExchangeResponse> exchangeDataPlaneToken(String controlPlaneToken) {
    return webClientFactory.getWebClient()
        .postAbs(CCloudOAuthConfig.CCLOUD_DATA_PLANE_TOKEN_URI)
        .putHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .putHeader(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", controlPlaneToken))
        .sendBuffer(Buffer.buffer("{}"))
        .map(response -> {
          try {
            return OBJECT_MAPPER.readValue(
                response.bodyAsString(),
                DataPlaneTokenExchangeResponse.class);
          } catch (JsonProcessingException exception) {
            throw new CCloudAuthenticationFailedException(
                String.format(
                    "Could not parse the response from Confluent Cloud when exchanging the control "
                        + "plane token of the account %s for the data plane token.",
                    getUserEmail()),
                exception);
          }
        });
  }

  /**
   * Create a random Base64-encoded string.
   *
   * @param numberOfBytes The length of the random string in number of bytes.
   * @return The Base64-encoded random string.
   */
  private String createRandomEncodedString(int numberOfBytes) {
    byte[] code = new byte[numberOfBytes];
    SECURE_RANDOM.nextBytes(code);
    return Base64.encodeBase64URLSafeString(code);
  }

  /**
   * Create Code Challenge as defined in
   * <a href="https://datatracker.ietf.org/doc/html/rfc7636#section-4.2">Proof Key for Code
   * Exchange by OAuth Public Clients</a>.
   */
  private String createCodeChallenge() {
    try {
      byte[] bytes = codeVerifier.getBytes(StandardCharsets.US_ASCII);
      MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
      md.update(bytes, 0, bytes.length);
      byte[] digest = md.digest();
      return Base64.encodeBase64URLSafeString(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new CCloudAuthenticationFailedException("Could not generate the code challenge", e);
    }
  }

  private boolean isTokenMissing(Token token) {
    return (token == null || token.token() == null || token.token().isEmpty());
  }

  private record Tokens(
      Token refreshToken,
      Token controlPlaneToken,
      Token dataPlaneToken,
      UserDetails user,
      OrganizationDetails organization,
      Instant endOfLifetime,
      AuthErrors errors,
      Integer failedTokenRefreshAttempts
  ) {

    Tokens() {
      this(
          null,
          null,
          null,
          null,
          null,
          null,
          new AuthErrors(),
          0);
    }

    Optional<Instant> expiresAt() {
      return Stream
          .of(refreshToken, controlPlaneToken, dataPlaneToken)
          .filter(Objects::nonNull)
          .map(Token::expiresAt)
          .min(Instant::compareTo);
    }

    Tokens withRefreshToken(Token refreshToken) {
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          errors,
          failedTokenRefreshAttempts);
    }

    Tokens withControlPlaneToken(Token controlPlaneToken) {
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          errors,
          failedTokenRefreshAttempts);
    }

    Tokens withDataPlaneToken(Token dataPlaneToken) {
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          errors,
          failedTokenRefreshAttempts);
    }

    Tokens withUser(UserDetails user) {
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          errors,
          failedTokenRefreshAttempts);
    }

    Tokens withOrganization(OrganizationDetails organization) {
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          errors,
          failedTokenRefreshAttempts);
    }

    Tokens withEndOfLifetime(Instant endOfLifetime) {
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          errors,
          failedTokenRefreshAttempts);
    }

    Tokens withErrors(AuthErrors errors) {
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          errors,
          failedTokenRefreshAttempts);
    }

    Tokens withSuccessfulTokenRefreshAttempt() {
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          // Reset any errors related to the token refresh
          errors.withoutTokenRefresh(),
          // Reset the number of failed token refresh attempts
          0);
    }

    Tokens withFailedTokenRefreshAttempt(Throwable error) {
      boolean isTransient =
          failedTokenRefreshAttempts < CCloudOAuthConfig.MAX_TOKEN_REFRESH_ATTEMPTS
              && !error.getMessage().contains("Unknown or invalid refresh token.");
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          // Add error related to the token refresh
          errors.withTokenRefresh(error.getMessage(), isTransient),
          // Bump the number of failed token refresh attempts
          failedTokenRefreshAttempts + 1);
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record UserDetails(
      String id,
      String email,
      @JsonProperty(value = "first_name") String firstName,
      @JsonProperty(value = "last_name") String lastName,
      @JsonProperty(value = "resource_id") String resourceId,
      @JsonProperty(value = "service_account") boolean serviceAccount,
      @JsonProperty(value = "social_connection") String socialConnection,
      @JsonProperty(value = "auth_type") String authType
  ) {

    public CCloud.UserId getId() {
      return resourceId != null ? new CCloud.UserId(resourceId) : null;
    }

    public ConnectionStatus.UserInfo asUserInfo() {
      return new ConnectionStatus.UserInfo(
          resourceId,
          email,
          firstName,
          lastName,
          socialConnection,
          authType);
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record SsoDetails(
      boolean enabled,
      @JsonProperty(value = "auth0_connection_name") String auth0ConnectionName,
      @JsonProperty(value = "tenant_id") String tenantId,
      @JsonProperty(value = "multi_tenant") boolean multiTenant,
      String mode,
      @JsonProperty(value = "connection_name") String connectionName,
      String vendor,
      @JsonProperty(value = "jit_enabled") boolean jitEnabled,
      @JsonProperty(value = "bup_enabled") boolean bupEnabled
  ) {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record OrganizationDetails(
      String id,
      @JsonProperty(value = "resource_id") String resourceId,
      String name,
      SsoDetails sso
  ) {

    public CCloud.OrganizationId getId() {
      return resourceId != null ? new CCloud.OrganizationId(resourceId) : null;
    }
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record IdTokenExchangeResponse(
      @JsonProperty(value = "access_token") String accessToken,
      @JsonProperty(value = "refresh_token") String refreshToken,
      @JsonProperty(value = "id_token") String idToken,
      String scope,
      @JsonProperty(value = "expires_in") long expiresIn,
      @JsonProperty(value = "token_type") String tokenType,
      JsonNode error,
      @JsonProperty(value = "error_description") JsonNode errorDescription
  ) {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record ControlPlaneTokenExchangeResponse(
      String token,
      JsonNode error,
      UserDetails user,
      OrganizationDetails organization,
      @JsonProperty(value = "refresh_token") String refreshToken,
      @JsonProperty(value = "identity_provider") JsonNode identityProvider) {

  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record DataPlaneTokenExchangeResponse(
      JsonNode error,
      String token,
      @JsonProperty(value = "regional_token") String regionalToken
  ) {
  }

  @RegisterForReflection
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record CheckJwtResponse(JsonNode error, JsonNode claims) {

  }

  @RegisterForReflection
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  record ExchangeControlPlaneTokenRequest(
      @JsonProperty(value = "id_token", required = true) String idToken,
      @JsonProperty("org_resource_id") String orgResourceId
  ) {

    String toJsonString() {
      JsonObject jsonObject = new JsonObject();
      jsonObject.put("id_token", idToken);
      Optional.ofNullable(orgResourceId).ifPresent(
          resourceId -> jsonObject.put("org_resource_id", resourceId));
      return jsonObject.toString();
    }
  }
}
