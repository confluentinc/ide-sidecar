package io.confluent.idesidecar.restapi.auth;

import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.exceptions.CCloudAuthenticationFailedException;
import io.confluent.idesidecar.restapi.exceptions.TransientServerErrorException;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.util.CCloud;
import io.confluent.idesidecar.restapi.util.CCloudHttpSender;
import io.confluent.idesidecar.restapi.util.ObjectMapperFactory;
import io.confluent.idesidecar.restapi.util.RateLimitState;
import io.confluent.idesidecar.restapi.util.TimeUtil;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkus.arc.Arc;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.HttpCookie;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Supplier;
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
  private final AtomicReference<InFlightRefresh> inFlightRefresh = new AtomicReference<>();

  private final AtomicReference<Tokens> tokens = new AtomicReference<>(new Tokens());
  private final UriUtil uriUtil = new UriUtil();
  private final WebClientFactory webClientFactory;
  private final CCloudHttpSender httpSender;
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

    // Look up application-scoped beans via Arc when the CDI container is available; fall back to
    // a manual WebClientFactory + null httpSender for non-CDI tests that construct this context
    // directly. {@link #send} passes through without pacing when httpSender is null.
    var container = Arc.container();
    if (container != null) {
      webClientFactory = container.select(WebClientFactory.class).get();
      httpSender = container.select(CCloudHttpSender.class).get();
    } else {
      // WARN so a hit from an unexpected production path (partial Arc init, misconfigured native
      // image) is visible rather than silently disabling rate limiting.
      Log.warnf("Arc container unavailable during CCloudOAuthContext init; CCloud rate limiting"
          + " and 429 retry are disabled for this context. Expected only in non-CDI tests.");
      webClientFactory = new WebClientFactory();
      httpSender = null;
    }
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

      var uri = CCloudOAuthConfig.CCLOUD_CONTROL_PLANE_CHECK_JWT_URI;
      return send(uri, () -> webClientFactory.getWebClient()
          .getAbs(uri)
          .timeout(WebClientFactory.WEBCLIENT_REQUEST_TIMEOUT_MS)
          .putHeaders(getControlPlaneAuthenticationHeaders())
          // Synchronously tries to DNS resolve the hostname before sending the request.
          // Calling threads beware!
          .send())
          .map(result -> {
            if (result.statusCode() >= 500) {
              // says nothing about the session, so the caller can keep its last status
              throw new TransientServerErrorException(
                  uri,
                  result.statusCode(),
                  RateLimitState.parseRetryAfterHeader(result),
                  new CCloudAuthenticationFailedException((
                      "Confluent Cloud answered HTTP %d when verifying the authentication status "
                          + "of this connection."
                  ).formatted(result.statusCode()))
              );
            }
            try {
              var response = OBJECT_MAPPER.readValue(result.bodyAsString(), CheckJwtResponse.class);
              if (response.error() == null) {
                throw new CCloudAuthenticationFailedException(
                    "Confluent Cloud's response when verifying the authentication status of this "
                        + "connection had no error field."
                );
              }
              if (!response.error().isNull()) {
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
    return refreshOnce(organizationId, true);
  }

  /**
   * Like {@link #refresh(String)}, but a failure doesn't count as a failed token refresh attempt.
   */
  public Future<AuthContext> refreshIgnoreFailures(String organizationId) {
    return refreshOnce(organizationId, false);
  }

  /**
   * Whether a token refresh is running for this connection.
   */
  boolean isRefreshInFlight() {
    return inFlightRefresh.get() != null;
  }

  /**
   * Start a refresh, or join the one already running for the same organization, so this
   * connection never runs two at once. CCloud rotates the refresh token on each use, so a second
   * refresh sent with the old token would fail. A refresh for a different organization starts
   * once the running one finishes, and reads the refresh token only then. Several such waiters
   * race to start next, in no particular order.
   */
  private Future<AuthContext> refreshOnce(String organizationId, boolean countFailures) {
    Promise<AuthContext> promise = Promise.promise();
    var mine = new InFlightRefresh(organizationId, promise.future());
    var running = inFlightRefresh.compareAndExchange(null, mine);
    if (running != null) {
      return Objects.equals(running.organizationId(), organizationId)
          ? running.future()
          : running.future().transform(done -> refreshOnce(organizationId, countFailures));
    }
    Future<AuthContext> chain;
    writeLock.lock();
    try {
      chain = withDeadline(bareRefresh(organizationId))
          // Reset any existing error related to token refresh flow
          .onSuccess(onSuccessfulTokenRefresh());
      if (countFailures) {
        // Persist error related to token refresh flow
        chain = chain.onFailure(onFailedTokenRefresh());
      }
    } catch (RuntimeException e) {
      // don't leave a refresh that never started marked as running
      inFlightRefresh.compareAndSet(mine, null);
      Log.errorf(e, "Couldn't start the CCloud token refresh");
      promise.fail(e);
      return promise.future();
    } finally {
      writeLock.unlock();
    }
    // after the handlers above, so the backoff is recorded before another refresh can start
    chain.onComplete(result -> {
      inFlightRefresh.compareAndSet(mine, null);
      promise.handle(result);
    });
    return promise.future();
  }

  /**
   * Fail {@code refresh} once {@link #refreshDeadline()} has passed, so it can't hold the
   * single-flight slot forever. The refresh itself keeps going, and stores tokens as each step
   * succeeds: CCloud may already have rotated the refresh token, so a late success is kept and
   * clears the failure count, and a late failure is only logged, since the timeout already
   * stood for it.
   */
  private Future<AuthContext> withDeadline(Future<AuthContext> refresh) {
    Promise<AuthContext> bounded = Promise.promise();
    var deadline = refreshDeadline();
    var timer = CompletableFuture.runAsync(
        () -> {
          var message = "The CCloud token refresh didn't finish within %s"
              .formatted(TimeUtil.humanReadableDuration(deadline));
          if (bounded.tryFail(new TimeoutException(message))) {
            Log.error(message);
          }
        },
        CompletableFuture.delayedExecutor(deadline.toMillis(), TimeUnit.MILLISECONDS)
    );
    refresh.onComplete(result -> {
      timer.cancel(false);
      var inTime = result.succeeded()
          ? bounded.tryComplete(result.result())
          : bounded.tryFail(result.cause());
      if (inTime) {
        return;
      }
      if (result.succeeded()) {
        Log.warn("A CCloud token refresh finished after its deadline; keeping its new tokens");
        onSuccessfulTokenRefresh().handle(result.result());
      } else {
        Log.errorf(
            result.cause(),
            "A CCloud token refresh failed after its deadline had passed"
        );
      }
    });
    return bounded.future();
  }

  /**
   * The refresh running for this connection, and the organization it's for.
   */
  private record InFlightRefresh(String organizationId, Future<AuthContext> future) {

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
          // the refresh token's lifetime is still unmeasured; log it, never the tokens
          .onSuccess(response -> {
            if (response.error() == null || response.error().isNull()) {
              Log.infof(
                  "CCloud sign-in token exchange returned expires_in=%ds", response.expiresIn()
              );
            }
          })
          .compose(response -> this.processTokenExchangeResponse(response, ccloudOrganizationId))
          .map(CCloudOAuthContext.class::cast)
          .onSuccess(result -> {
            // Reset any existing errors
            tokens.updateAndGet(oldTokens ->
                oldTokens
                    .withErrors(
                        oldTokens.errors
                            .withoutSignIn()
                            .withoutTokenRefresh()
                            .withoutAuthStatusCheck()
                    )
                    // a new sign-in starts with no failed refreshes
                    .withSuccessfulTokenRefreshAttempt()
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
   * refresh token is valid and has not yet reached its absolute lifetime, (2) no failed refresh
   * has made the connection fail (see {@link #isTransientRefreshFailure}), (3) we're not still
   * waiting out the backoff after a failed refresh, (4) no refresh is already running, and (5)
   * the tokens' refresh point (see {@link #refreshDueAt}) has passed, or at least one token of
   * the connection will expire before the next run of this job.
   *
   * @return if we should attempt the token refresh.
   */
  public boolean shouldAttemptTokenRefresh() {
    readLock.lock();
    try {
      // Perform token refresh only if auth context will expire before next run of this job
      var now = now();
      var expiresAt = expiresAt();
      var nextExecution = now.plus(CCloudOAuthConfig.TOKEN_REFRESH_INTERVAL_SECONDS);
      var atLeastOneTokenWillExpireBeforeNextRun = expiresAt.isPresent()
          && expiresAt.get().compareTo(nextExecution) < 0;
      var current = tokens.get();
      var nextAttemptAt = current.refreshFailures.nextAttemptAt();
      var backingOff = nextAttemptAt != null && now.isBefore(nextAttemptAt);
      var refreshDue = current.refreshDueAt != null && !now.isBefore(current.refreshDueAt);

      return !hasReachedEndOfLifetime()
          && !hasNonTransientError()
          && !backingOff
          && !isRefreshInFlight()
          && (refreshDue || atLeastOneTokenWillExpireBeforeNextRun);
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
    return endOfLifetime != null && now().compareTo(endOfLifetime) >= 0;
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
    return tokens.get().refreshFailures.attempts();
  }

  /**
   * When the next token refresh may start after a failed one, or empty if the last one didn't
   * fail.
   */
  Optional<Instant> getNextTokenRefreshAttemptAt() {
    return Optional.ofNullable(tokens.get().refreshFailures.nextAttemptAt());
  }

  /**
   * The current time, read through a method so tests can control it.
   */
  Instant now() {
    return Instant.now();
  }

  /**
   * How long a refresh may run before it counts as failed, read through a method so tests can
   * shorten it.
   */
  Duration refreshDeadline() {
    return CCloudOAuthConfig.TOKEN_REFRESH_DEADLINE;
  }

  /**
   * Consecutive failed token refreshes since the last successful one or sign-in, when the first
   * of them failed, and when the next attempt may start.
   */
  record RefreshFailures(int attempts, Instant firstFailedAt, Instant nextAttemptAt) {

    static final RefreshFailures NONE = new RefreshFailures(0, null, null);

    /**
     * The failures after one more at {@code now}, with {@code random} in [0, 1) for the jitter.
     */
    RefreshFailures after(Instant now, double random) {
      var count = attempts + 1;
      return new RefreshFailures(
          count,
          firstFailedAt != null ? firstFailedAt : now,
          now.plus(refreshBackoff(count, random))
      );
    }
  }

  /**
   * How long a token is valid, from its JWT claims: {@code exp - iat} when both are present, so a
   * wrong local clock doesn't matter, else {@code exp} minus now. Falls back to the configured
   * lifetime, with a warning, when the token isn't a JWT or has no usable expiry.
   */
  static Duration tokenLifetime(String jwt, String tokenName, Instant now) {
    var fallback = CCloudOAuthConfig.CCLOUD_CONTROL_PLANE_TOKEN_LIFE_TIME_SEC;
    var parts = jwt == null ? new String[0] : jwt.split("\\.");
    if (parts.length < 2) {
      Log.warnf("The %s token isn't a JWT; assuming it lasts %s", tokenName, fallback);
      return fallback;
    }
    JsonNode claims;
    try {
      claims = OBJECT_MAPPER.readTree(java.util.Base64.getUrlDecoder().decode(parts[1]));
    } catch (IllegalArgumentException | IOException e) {
      // the exception's message can quote the claims, so log only its type
      Log.warnf(
          "Couldn't read the %s token's claims (%s); assuming it lasts %s",
          tokenName, e.getClass().getSimpleName(), fallback
      );
      return fallback;
    }
    var exp = claims.get("exp");
    var iat = claims.get("iat");
    if (exp == null || !exp.canConvertToLong()) {
      Log.warnf("The %s token has no expiry; assuming it lasts %s", tokenName, fallback);
      return fallback;
    }
    var lifetime = iat != null && iat.canConvertToLong()
        ? Duration.ofSeconds(exp.asLong() - iat.asLong())
        : Duration.between(now, Instant.ofEpochSecond(exp.asLong()));
    if (lifetime.isNegative()
        || lifetime.isZero()
        || lifetime.compareTo(CCloudOAuthConfig.CCLOUD_REFRESH_TOKEN_ABSOLUTE_LIFETIME) > 0) {
      // longer than the whole session means a malformed expiry, such as one in milliseconds
      Log.warnf(
          "The %s token's expiry gives a lifetime of %s; assuming it lasts %s",
          tokenName, lifetime, fallback
      );
      return fallback;
    }
    return lifetime;
  }

  /**
   * When to refresh tokens whose shortest lifetime is {@code shortestLifetime}: a random point
   * between 70% and 90% of it, with {@code random} in [0, 1), so connections don't all refresh at
   * once and there's time to retry before the tokens expire.
   */
  static Instant refreshDueAt(Instant now, Duration shortestLifetime, double random) {
    return now.plusMillis(Math.round(shortestLifetime.toMillis() * (0.7 + 0.2 * random)));
  }

  /**
   * How long to wait after the given number of consecutive failed refreshes: the initial backoff,
   * multiplied for each failure after the first and capped, plus up to the jitter factor on top.
   */
  static Duration refreshBackoff(int failedAttempts, double random) {
    var millis = Math.min(
        CCloudOAuthConfig.TOKEN_REFRESH_MAX_BACKOFF.toMillis(),
        CCloudOAuthConfig.TOKEN_REFRESH_INITIAL_BACKOFF.toMillis()
            * Math.pow(CCloudOAuthConfig.TOKEN_REFRESH_BACKOFF_MULTIPLIER, failedAttempts - 1)
    );
    return Duration.ofMillis(
        Math.round(millis * (1 + CCloudOAuthConfig.TOKEN_REFRESH_JITTER_FACTOR * random))
    );
  }

  /**
   * Whether a failed refresh should be retried: not after the attempt limit or once the failures
   * have gone on longer than the budget, and never for a refresh token the server rejected.
   */
  static boolean isTransientRefreshFailure(
      RefreshFailures failures,
      Throwable error,
      Instant now
  ) {
    var message = error.getMessage();
    return failures.attempts() < CCloudOAuthConfig.MAX_TOKEN_REFRESH_ATTEMPTS
        && Duration.between(failures.firstFailedAt(), now)
            .compareTo(CCloudOAuthConfig.TOKEN_REFRESH_FAILURE_BUDGET) < 0
        && (message == null || !message.contains("Unknown or invalid refresh token."));
  }

  /**
   * The error a failed refresh records: the failure's own message while refreshes will be
   * retried, and why they stopped once they won't.
   */
  static String refreshFailureMessage(RefreshFailures failures, Throwable error, Instant now) {
    if (isTransientRefreshFailure(failures, error, now)) {
      return error.getMessage();
    }
    return ("Gave up refreshing the Confluent Cloud tokens after %d failed %s over %s; "
        + "sign in again. Last error: %s").formatted(
        failures.attempts(),
        failures.attempts() == 1 ? "attempt" : "attempts",
        TimeUtil.humanReadableDuration(Duration.between(failures.firstFailedAt(), now)),
        error.getMessage()
    );
  }

  private Handler<Throwable> onFailedTokenRefresh() {
    return failure -> {
      // outside the update, which can run more than once
      var now = now();
      var random = ThreadLocalRandom.current().nextDouble();
      var before = new AtomicReference<Tokens>();
      var after = tokens.updateAndGet(oldTokens -> {
        before.set(oldTokens);
        return oldTokens.withFailedTokenRefreshAttempt(failure, now, random);
      });
      var gaveUp = after.errors.tokenRefresh();
      var gaveUpBefore = before.get().errors.tokenRefresh();
      if (gaveUp != null && Boolean.FALSE.equals(gaveUp.isTransient())
          && (gaveUpBefore == null || !Boolean.FALSE.equals(gaveUpBefore.isTransient()))) {
        Log.errorf(failure, "%s", gaveUp.message());
      }
    };
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

  /**
   * Route a Future-based send through {@link CCloudHttpSender} when available; passthrough when
   * {@code httpSender} is null (non-CDI test construction).
   */
  private Future<HttpResponse<Buffer>> send(
      String url,
      Supplier<Future<HttpResponse<Buffer>>> sender) {
    return httpSender != null ? httpSender.send(url, sender) : sender.get();
  }

  private Future<IdTokenExchangeResponse> performTokenExchange(String requestBody) {
    var uri = CCloudOAuthConfig.CCLOUD_OAUTH_TOKEN_URI;
    return send(uri, () -> webClientFactory.getWebClient()
        .postAbs(uri)
        .timeout(WebClientFactory.WEBCLIENT_REQUEST_TIMEOUT_MS)
        .putHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED)
        .sendBuffer(Buffer.buffer(requestBody)))
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

    final var now = now();
    // the refresh token lasts until the connection's end of lifetime, whatever expires_in says
    tokens.updateAndGet(oldTokens ->
        oldTokens.withRefreshToken(
            new Token(
                idTokenExchangeResponse.refreshToken(),
                oldTokens.endOfLifetime != null
                    ? oldTokens.endOfLifetime
                    : now.plus(CCloudOAuthConfig.CCLOUD_REFRESH_TOKEN_ABSOLUTE_LIFETIME)
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
                          now.plus(tokenLifetime(sessionTokenResult.token(), "control plane", now))
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

          var dataPlaneTokenValue = dataPlaneTokenExchangeResponseResult.token();
          var dataPlaneToken = new Token(
              dataPlaneTokenValue,
              now.plus(tokenLifetime(dataPlaneTokenValue, "data plane", now))
          );
          // outside the update, which can run more than once
          var random = ThreadLocalRandom.current().nextDouble();
          tokens.updateAndGet(oldTokens -> {
            var shortestExpiry = oldTokens.controlPlaneToken != null
                && oldTokens.controlPlaneToken.expiresAt().isBefore(dataPlaneToken.expiresAt())
                ? oldTokens.controlPlaneToken.expiresAt()
                : dataPlaneToken.expiresAt();
            return oldTokens
                .withDataPlaneToken(dataPlaneToken)
                .withRefreshDueAt(
                    refreshDueAt(now, Duration.between(now, shortestExpiry), random)
                );
          });

          return Future.succeededFuture(this);
        });
  }

  public Future<ControlPlaneTokenExchangeResponse> exchangeControlPlaneToken(
      ExchangeControlPlaneTokenRequest request) {
    var uri = CCloudOAuthConfig.CCLOUD_CONTROL_PLANE_TOKEN_URI;
    return send(uri, () -> webClientFactory.getWebClient()
        .postAbs(uri)
        .timeout(WebClientFactory.WEBCLIENT_REQUEST_TIMEOUT_MS)
        .putHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .sendBuffer(Buffer.buffer(request.toJsonString())))
        .map(response -> {
          try {
            ControlPlaneTokenExchangeResponse responseBody =
                OBJECT_MAPPER.readValue(response.bodyAsString(),
                    ControlPlaneTokenExchangeResponse.class);

            String setCookieHeader = response.getHeader("Set-Cookie");
            if (setCookieHeader != null && !setCookieHeader.isEmpty()) {
              List<HttpCookie> cookies = HttpCookie.parse(setCookieHeader);
              String authToken = cookies.stream()
                  .filter(cookie -> cookie.getName().equals("auth_token"))
                  .findFirst()
                  .orElseThrow(() ->
                      new CCloudAuthenticationFailedException(
                          "Auth token not found in response from Confluent Cloud: Set-Cookie header present but no token found."
                      )
                  )
                  .getValue();
              return responseBody.withToken(authToken);
            } else {
              throw new CCloudAuthenticationFailedException(
                  "Set-Cookie header not found in response from Confluent Cloud.");
            }
          } catch (JsonProcessingException e) {
            throw new CCloudAuthenticationFailedException(
                "Could not parse the response from Confluent Cloud when retrieving the control plane token.", e);
          }
        });
  }

  private Future<DataPlaneTokenExchangeResponse> exchangeDataPlaneToken(String controlPlaneToken) {
    var uri = CCloudOAuthConfig.CCLOUD_DATA_PLANE_TOKEN_URI;
    return send(uri, () -> webClientFactory.getWebClient()
        .postAbs(uri)
        .timeout(WebClientFactory.WEBCLIENT_REQUEST_TIMEOUT_MS)
        .putHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .putHeader(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", controlPlaneToken))
        .sendBuffer(Buffer.buffer("{}")))
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
      RefreshFailures refreshFailures,
      Instant refreshDueAt
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
          RefreshFailures.NONE,
          null);
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
          refreshFailures,
          refreshDueAt);
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
          refreshFailures,
          refreshDueAt);
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
          refreshFailures,
          refreshDueAt);
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
          refreshFailures,
          refreshDueAt);
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
          refreshFailures,
          refreshDueAt);
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
          refreshFailures,
          refreshDueAt);
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
          refreshFailures,
          refreshDueAt);
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
          // Reset the failed token refresh attempts and their backoff
          RefreshFailures.NONE,
          refreshDueAt);
    }

    Tokens withFailedTokenRefreshAttempt(Throwable error, Instant now, double random) {
      var failures = refreshFailures.after(now, random);
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          // Add error related to the token refresh
          errors.withTokenRefresh(
              refreshFailureMessage(failures, error, now),
              isTransientRefreshFailure(failures, error, now)
          ),
          failures,
          refreshDueAt);
    }

    Tokens withRefreshDueAt(Instant refreshDueAt) {
      return new Tokens(
          refreshToken,
          controlPlaneToken,
          dataPlaneToken,
          user,
          organization,
          endOfLifetime,
          errors,
          refreshFailures,
          refreshDueAt);
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

    public ControlPlaneTokenExchangeResponse withToken(String token) {
      return new ControlPlaneTokenExchangeResponse(
          token,
          error,
          user,
          organization,
          refreshToken,
          identityProvider);
    }

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
