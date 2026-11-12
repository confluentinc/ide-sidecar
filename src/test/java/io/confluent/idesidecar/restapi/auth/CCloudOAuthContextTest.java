package io.confluent.idesidecar.restapi.auth;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext.ExchangeControlPlaneTokenRequest;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.CCloudAuthenticationFailedException;
import io.confluent.idesidecar.restapi.exceptions.TooManyRequestsException;
import io.confluent.idesidecar.restapi.exceptions.TransientServerErrorException;
import io.confluent.idesidecar.restapi.util.CCloud;
import io.confluent.idesidecar.restapi.util.CCloudApiRateLimiter;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import jakarta.inject.Inject;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.mockito.Mockito;

@QuarkusTest
@ConnectWireMock
class CCloudOAuthContextTest {

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  CCloudApiRateLimiter rateLimiter;

  WireMock wireMock;

  CCloudTestUtil ccloudTestUtil;

  private static final int AWAIT_COMPLETION_TIMEOUT_SEC = 5;
  // Rate-limit tests pay ~1s of bucket-wait per 429 (resetSeconds clamps to 1.0 minimum), so a
  // single retry + a sustained-429 exhaustion sequence both need a more generous deadline.
  private static final int RATE_LIMIT_TIMEOUT_SEC = 15;

  private static final String FAKE_AUTHORIZATION_CODE = "fake_authorization_code";

  @BeforeEach
  void registerWireMockRoutes() {
    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
    ccloudTestUtil.registerWireMockRoutesForCCloudOAuth(
        FAKE_AUTHORIZATION_CODE, null, null);
    // Limiter is @ApplicationScoped: clear leftover bucket state from prior tests so per-test
    // 429 sequences start from a clean slate.
    rateLimiter.reset();
  }

  @AfterEach
  void cleanUp() {
    connectionStateManager.clearAllConnectionStates();
    wireMock.removeMappings();
  }

  @Test
  void shouldInitializeTheOauthState() {
    var authContext = new CCloudOAuthContext();
    assertFalse(authContext.getOauthState().isEmpty());
  }

  @Test
  void shouldAssignUniqueOauthStates() {
    var firstContext = new CCloudOAuthContext();
    var secondContext = new CCloudOAuthContext();
    var thirdContext = new CCloudOAuthContext();

    assertNotEquals(firstContext.getOauthState(), secondContext.getOauthState());
    assertNotEquals(firstContext.getOauthState(), thirdContext.getOauthState());
    assertNotEquals(secondContext.getOauthState(), thirdContext.getOauthState());
  }

  @Test
  void createTokensFromAuthorizationCodeShouldExchangeControlAndDataPlaneTokens() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.succeeding(authenticatedContext ->
                testContext.verify(() -> {
                  assertNotNull(authenticatedContext.getControlPlaneToken());
                  assertNotNull(authenticatedContext.getDataPlaneToken());
                  assertNotNull(authenticatedContext.getCurrentOrganization());
                  assertEquals(
                      new CCloud.OrganizationId("23b1185e-d874-4f61-81d6-c9c61aa8969c"),
                      authenticatedContext.getCurrentOrganization().getId()
                  );
                  assertEquals(
                      new CCloud.UserId("u-jg9zxp"),
                      authenticatedContext.getUser().getId()
                  );
                  // Error related to sign in should not be present
                  assertNull(authenticatedContext.getErrors().signIn());
                }).completeNow()));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void exchangeControlPlaneTokenShouldFailIfCookieHeaderIsEmpty() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    wireMock.register(
        WireMock
            .post("/api/sessions")
            .willReturn(
                WireMock
                    .aResponse()
                    .withHeader("Set-Cookie", "")
                    .withStatus(201)
                    .withBody("{}")
            ));

    var request = new ExchangeControlPlaneTokenRequest("valid_id_token", null);

    authContext.exchangeControlPlaneToken(request)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Set-Cookie header not found in response from Confluent Cloud.",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  testContext.completeNow();
                })));

            assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

            if (testContext.failed()) {
                  throw testContext.causeOfFailure();
                }
  }

  @Test
  void exchangeControlPlaneTokenShouldFailIfCookieHeaderIsNotPresent() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    wireMock.register(
        WireMock
            .post("/api/sessions")
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody("{}")));

    var request = new ExchangeControlPlaneTokenRequest("valid_id_token", null);

    authContext.exchangeControlPlaneToken(request)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Set-Cookie header not found in response from Confluent Cloud.",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  testContext.completeNow();
                })));
        assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

        if (testContext.failed()) {
             throw testContext.causeOfFailure();
              }
  }

  @Test
  void createTokensFromAuthorizationCodeShouldReturnFailedFutureIfControlPlaneTokenExchangeFailed()
      throws Throwable {

    wireMock.register(
        WireMock
            .post("/api/sessions")
            .willReturn(
                WireMock
                    .aResponse()
                    .withHeader("Set-Cookie", "auth_token=" + "bad_token")
                    .withStatus(201)
                    .withBody("{\"error\":{\"code\":401,\"message\":\"Unauthorized\"}}")
            ));

    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Retrieving the control plane token failed for the following reason: "
                          + "{\"code\":401,\"message\":\"Unauthorized\"}",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  // Error related to sign in should be present
                  assertNotNull(authContext.getErrors().signIn());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void createTokensFromAuthorizationCodeShouldReturnFailedFutureIfCptExchangeReturnsInvalidJson()
      throws Throwable {

    wireMock.register(
        WireMock
            .post("/api/sessions")
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody("invalid_json")
            ));

    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Could not parse the response from Confluent Cloud when retrieving "
                          + "the control plane token.",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  // Error related to sign in should be present
                  assertNotNull(authContext.getErrors().signIn());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void createTokensFromAuthorizationCodeShouldReturnFailedFutureIfTokenExchangeReturnsInvalidJson()
      throws Throwable {

    wireMock.register(
        WireMock
            .post("/oauth/token")
            .willReturn(
                WireMock
                    .aResponse()
                    .withBody("{\"error\": \"invalid_request\", \"error_description\": \"Mock error response\"}")
                    .withStatus(201)

            ).atPriority(100));
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Retrieving the ID token failed for the following reason: "
                          + "invalid_request - Mock error response",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  // Error related to sign in should be present
                  assertNotNull(authContext.getErrors().signIn());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void createTokensFromAuthorizationCodeShouldReturnFailedFutureIfDataPlaneTokenExchangeFailed()
      throws Throwable {

    wireMock.register(
        WireMock
            .post("/api/access_tokens")
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody("{\"error\":{\"code\":401,\"message\":\"Unauthorized\"}}")
            ).atPriority(100));

    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Retrieving the data plane token for the account "
                          + "ssprenger+test@confluent.io failed: "
                          + "{\"code\":401,\"message\":\"Unauthorized\"}",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  // Error related to sign in should be present
                  assertNotNull(authContext.getErrors().signIn());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void createTokensFromAuthorizationCodeShouldReturnFailedFutureIfDptExchangeReturnsInvalidJson()
      throws Throwable {

    wireMock.register(
        WireMock
            .post("/api/access_tokens")
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody("invalid_json")
            ).atPriority(100));

    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Could not parse the response from Confluent Cloud when exchanging"
                          + " the control plane token of the account ssprenger+test@confluent.io for"
                          + " the data plane token.",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  // Error related to sign in should be present
                  assertNotNull(authContext.getErrors().signIn());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void shouldAttemptTokenRefreshShouldReturnFalseIfRefreshTokenIsMissing() {
    // By default, the CCloudOAuthContext does not hold any tokens
    var authContext = new CCloudOAuthContext();
    assertFalse(authContext.shouldAttemptTokenRefresh());
  }

  @Test
  void shouldAttemptTokenRefreshShouldReturnFalseIfRefreshTokenHasExpired() {
    var authContext = Mockito.spy(CCloudOAuthContext.class);

    // refresh token has already expired
    Mockito.when(authContext.expiresAt())
        .thenReturn(Optional.of(Instant.now().minusSeconds(5)));
    Mockito.when(authContext.getEndOfLifetime())
        .thenReturn(Instant.now().minusSeconds(5));

    assertFalse(authContext.shouldAttemptTokenRefresh());
  }

  @Test
  void shouldAttemptTokenRefreshShouldReturnFalseIfNonTransientErrorHasBeenExperienced() {
    var authContext = Mockito.spy(CCloudOAuthContext.class);

    // tokens have not yet expired
    Mockito.when(authContext.expiresAt())
        .thenReturn(Optional.of(Instant.now().plusSeconds(5)));
    Mockito.when(authContext.getEndOfLifetime())
        .thenReturn(Instant.now().plusSeconds(5));

    // connection has experienced non transient error
    Mockito.when(authContext.hasNonTransientError()).thenReturn(true);

    assertFalse(authContext.shouldAttemptTokenRefresh());
  }

  @Test
  void hasReachedEndOfLifetimeShouldReturnTrueIfEndOfLifetimeIsReached() {
    var authContext = Mockito.spy(CCloudOAuthContext.class);
    var endOfLifetime = Instant.now().minusSeconds(1);

    Mockito.when(authContext.getEndOfLifetime()).thenReturn(endOfLifetime);

    assertTrue(authContext.hasReachedEndOfLifetime());
  }

  @Test
  void hasReachedEndOfLifetimeShouldReturnFalseIfEndOfLifetimeIsNotReached() {
    var authContext = Mockito.spy(CCloudOAuthContext.class);
    var endOfLifetime = Instant.now().plusSeconds(60);

    Mockito.when(authContext.getEndOfLifetime()).thenReturn(endOfLifetime);

    assertFalse(authContext.hasReachedEndOfLifetime());
  }

  @Test
  void hasReachedEndOfLifetimeShouldReturnFalseIfEndOfLifetimeIsNull() {
    var authContext = Mockito.spy(CCloudOAuthContext.class);

    Mockito.when(authContext.getEndOfLifetime()).thenReturn(null);

    assertFalse(authContext.hasReachedEndOfLifetime());
  }

  @Test
  // TODO: Figure out why this consistently fails on Windows
  @DisabledIfSystemProperty(named = "os.name", matches = ".*Windows.*")
  void shouldAttemptTokenRefreshShouldReturnTrueForConnectionsEligibleForATokenRefreshAttempt() {
    var authContext = Mockito.spy(CCloudOAuthContext.class);

    // tokens have not yet expired
    Mockito.when(authContext.expiresAt())
        .thenReturn(Optional.of(Instant.now().plusSeconds(5)));
    Mockito.when(authContext.getEndOfLifetime())
        .thenReturn(Instant.now().plusSeconds(5));

    // has not experienced too many token refresh attempts
    Mockito.when(authContext.getFailedTokenRefreshAttempts())
        .thenReturn(0);

    assertTrue(authContext.shouldAttemptTokenRefresh());
  }

  @Test
  void checkAuthenticationStatusShouldReturnTrueIfTheControlPlaneTokenIsValid() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .compose(AuthContext::checkAuthenticationStatus)
        .onComplete(
            testContext.succeeding(isAuthenticated ->
                testContext.verify(() -> {
                  assertTrue(isAuthenticated);
                  // Error related to auth status check should not be present
                  assertNull(authContext.getErrors().authStatusCheck());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void checkAuthenticationStatusShouldReturnFalseIfCCloudReturnsAnError() throws Throwable {
    var testContext = new VertxTestContext();

    wireMock.register(
        WireMock.get("/api/check_jwt")
            .willReturn(
                WireMock.aResponse().withStatus(200)
                    .withBody("{\"error\":{\"code\":401,\"message\":\"Unauthorized\"}}"))
            .atPriority(50)
    );

    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .compose(AuthContext::checkAuthenticationStatus)
        .onComplete(
            testContext.succeeding(isAuthenticated ->
                testContext.verify(() -> {
                  assertFalse(isAuthenticated);
                  // Error related to auth status check should not be present
                  assertNull(authContext.getErrors().authStatusCheck());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void checkAuthenticationStatusShouldReturnFalseIfTheControlPlaneTokenIsAbsent() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.checkAuthenticationStatus()
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Cannot verify authentication status because no control plane token is "
                          + "available. It's likely that this connection has not yet completed the "
                          + "authentication with CCloud.",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  // Error related to auth status check should be present
                  assertNotNull(authContext.getErrors().authStatusCheck());
                }).completeNow()));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void checkAuthenticationStatusShouldReportAServerErrorAsTransient() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();
    wireMock.register(
        WireMock.get("/api/check_jwt")
            .willReturn(WireMock.aResponse().withStatus(503).withBody("<html>busy</html>"))
            .atPriority(50)
    );

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .compose(AuthContext::checkAuthenticationStatus)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  var serverError = assertInstanceOf(TransientServerErrorException.class, failure);
                  assertEquals(503, serverError.getStatus());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void checkAuthenticationStatusShouldFailClearlyIfCCloudLeavesOutTheErrorField()
      throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();
    wireMock.register(
        WireMock.get("/api/check_jwt")
            .willReturn(WireMock.aResponse().withStatus(200).withBody("{}"))
            .atPriority(50)
    );

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .compose(AuthContext::checkAuthenticationStatus)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertInstanceOf(CCloudAuthenticationFailedException.class, failure);
                  assertNotNull(authContext.getErrors().authStatusCheck());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void checkAuthenticationStatusShouldReturnFailedFutureIfCCloudReturnsInvalidJson()
      throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    // Override wiremock for /api/check_jwt to return invalid JSON
    wireMock.register(
        WireMock.get("/api/check_jwt")
            .willReturn(
                WireMock.aResponse().withStatus(200)
                    .withBody("invalid JSON"))
            .atPriority(50)
    );

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .compose(AuthContext::checkAuthenticationStatus)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Could not parse the response from Confluent Cloud when verifying the "
                          + "authentication status of this connection.",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  assertEquals(
                      "com.fasterxml.jackson.core.JsonParseException",
                      failure.getCause().getClass().getCanonicalName());
                  // Error related to auth status check should be present
                  assertNotNull(authContext.getErrors().authStatusCheck());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void expiresAtShouldReturnTheEarliestExpirationTime() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.succeeding(authenticatedAuthContext ->
                testContext.verify(() -> {
                  // we know that the control plane token expires first
                  assertEquals(
                      authenticatedAuthContext.getControlPlaneToken().expiresAt(),
                      authenticatedAuthContext.expiresAt().orElseThrow());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void expiresAtShouldReturnEmptyOptionalIfAllTokensAreMissing() {
    var authContext = new CCloudOAuthContext();

    assertTrue(authContext.expiresAt().isEmpty());
  }

  @Test
  void refreshShouldPerformTheTokenExchangeWithARefreshToken() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.succeeding(authenticatedAuthContext ->
                testContext.verify(() -> {
                  // TODO
                  authenticatedAuthContext.refresh(null);
                  // Error related to token refresh should not be present
                  assertNull(authenticatedAuthContext.getErrors().tokenRefresh());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void refreshBackoffShouldGrowByOnePointSixUpToTheCap() {
    var waits = IntStream.rangeClosed(1, 9)
        .mapToObj(attempt -> CCloudOAuthContext.refreshBackoff(attempt, 0.0).toMillis())
        .toList();

    assertEquals(
        List.of(5_000L, 8_000L, 12_800L, 20_480L, 32_768L, 52_429L, 60_000L, 60_000L, 60_000L),
        waits
    );
  }

  @Test
  void refreshBackoffShouldAddUpToTwentyPercentJitter() {
    assertEquals(6_000L, CCloudOAuthContext.refreshBackoff(1, 1.0).toMillis());
    assertEquals(72_000L, CCloudOAuthContext.refreshBackoff(9, 1.0).toMillis());
  }

  @Test
  void refreshFailuresShouldTrackTheFirstFailureAndTheNextAttempt() {
    var first = Instant.parse("2026-11-06T12:00:00Z");
    var second = first.plusSeconds(10);

    var failures = CCloudOAuthContext.RefreshFailures.NONE
        .after(first, 0.0)
        .after(second, 0.0);

    assertEquals(2, failures.attempts());
    assertEquals(first, failures.firstFailedAt());
    assertEquals(second.plusSeconds(8), failures.nextAttemptAt());
  }

  @Test
  void refreshFailureShouldStayTransientUntilTheLimitOrBudgetRunsOut() {
    var now = Instant.parse("2026-11-06T12:00:00Z");
    var error = new RuntimeException("Service Unavailable");

    var limit = CCloudOAuthConfig.MAX_TOKEN_REFRESH_ATTEMPTS;
    assertTrue(CCloudOAuthContext.isTransientRefreshFailure(failures(limit - 1, now), error, now));
    assertFalse(CCloudOAuthContext.isTransientRefreshFailure(failures(limit, now), error, now));
    assertFalse(CCloudOAuthContext.isTransientRefreshFailure(
        failures(2, now.minus(CCloudOAuthConfig.TOKEN_REFRESH_FAILURE_BUDGET)), error, now
    ));
    assertTrue(CCloudOAuthContext.isTransientRefreshFailure(
        failures(1, now), new RuntimeException(), now
    ));
  }

  @Test
  void refreshFailureShouldBeNonTransientForAnInvalidRefreshToken() {
    var now = Instant.parse("2026-11-06T12:00:00Z");

    assertFalse(CCloudOAuthContext.isTransientRefreshFailure(
        failures(1, now),
        new RuntimeException("invalid_grant - Unknown or invalid refresh token."),
        now
    ));
  }

  @Test
  void refreshFailureShouldBeTransientForAFailureWithNoMessage() {
    var now = Instant.parse("2026-11-06T12:00:00Z");

    assertTrue(CCloudOAuthContext.isTransientRefreshFailure(
        failures(1, now), new RuntimeException((String) null), now
    ));
  }

  @Test
  void refreshFailureMessageShouldSayWhyItGaveUp() {
    var firstFailedAt = Instant.parse("2026-11-06T12:00:00Z");
    var now = firstFailedAt.plus(Duration.ofMinutes(6));

    var message = CCloudOAuthContext.refreshFailureMessage(
        failures(CCloudOAuthConfig.MAX_TOKEN_REFRESH_ATTEMPTS, firstFailedAt),
        new RuntimeException("The timeout of 20000ms ran out"),
        now
    );

    assertEquals(
        "Gave up refreshing the Confluent Cloud tokens after 10 failed attempts over 6m; "
            + "sign in again. Last error: The timeout of 20000ms ran out",
        message
    );
  }

  @Test
  void refreshFailureMessageShouldLeaveOutTheTimeForAnImmediateRejection() {
    var now = Instant.parse("2026-11-06T12:00:00Z");

    var message = CCloudOAuthContext.refreshFailureMessage(
        failures(1, now),
        new RuntimeException("invalid_grant - Unknown or invalid refresh token."),
        now
    );

    assertEquals(
        "Gave up refreshing the Confluent Cloud tokens after 1 failed attempt; sign in again. "
            + "Last error: invalid_grant - Unknown or invalid refresh token.",
        message
    );
  }

  @Test
  void refreshFailureMessageShouldPassOnAFailureThatWillBeRetried() {
    var now = Instant.parse("2026-11-06T12:00:00Z");

    var message = CCloudOAuthContext.refreshFailureMessage(
        failures(1, now), new RuntimeException("Connection refused"), now
    );

    assertEquals("Connection refused", message);
  }

  @Test
  void refreshTokenShouldExpireAtTheEndOfTheSessionLifetime() throws Throwable {
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(authContext.getEndOfLifetime(), authContext.getRefreshToken().expiresAt());
  }

  @Test
  void shouldNotAttemptTokenRefreshWhileBackingOff() throws Throwable {
    var authContext = Mockito.spy(new CCloudOAuthContext());
    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    // just before the tokens expire, so a refresh is due
    var failedAt = authContext.expiresAt().orElseThrow().minusSeconds(1);
    Mockito.doReturn(failedAt).when(authContext).now();
    failTokenExchanges();

    awaitFailure(authContext.refresh(null));

    var nextAttempt = authContext.getNextTokenRefreshAttemptAt().orElseThrow();
    assertTrue(nextAttempt.isAfter(failedAt.plusSeconds(4)), "next attempt " + nextAttempt);
    assertFalse(authContext.shouldAttemptTokenRefresh());
    Mockito.doReturn(nextAttempt).when(authContext).now();
    assertTrue(authContext.shouldAttemptTokenRefresh());
  }

  @Test
  void connectionShouldFailOnceRefreshFailuresOutlastTheBudgetUntilTheNextSignIn()
      throws Throwable {
    var authContext = Mockito.spy(new CCloudOAuthContext());
    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    var firstFailure = Instant.now();
    Mockito.doReturn(firstFailure).when(authContext).now();
    failTokenExchanges();
    awaitFailure(authContext.refresh(null));
    assertFalse(authContext.hasNonTransientError());

    Mockito.doReturn(
        firstFailure.plus(CCloudOAuthConfig.TOKEN_REFRESH_FAILURE_BUDGET).plusSeconds(1)
    ).when(authContext).now();
    awaitFailure(authContext.refresh(null));

    assertTrue(authContext.hasNonTransientError());
    assertFalse(authContext.shouldAttemptTokenRefresh());
    wireMock.removeMappings();
    ccloudTestUtil.registerWireMockRoutesForCCloudOAuth(FAKE_AUTHORIZATION_CODE, null, null);
    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertFalse(authContext.hasNonTransientError());
  }

  @Test
  void signInShouldResetFailedTokenRefreshAttempts() throws Throwable {
    var authContext = new CCloudOAuthContext();
    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    failTokenExchanges();
    awaitFailure(authContext.refresh(null));
    assertEquals(1, authContext.getFailedTokenRefreshAttempts());
    wireMock.removeMappings();
    ccloudTestUtil.registerWireMockRoutesForCCloudOAuth(FAKE_AUTHORIZATION_CODE, null, null);

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(0, authContext.getFailedTokenRefreshAttempts());
    assertTrue(authContext.getNextTokenRefreshAttemptAt().isEmpty());
  }

  private void failTokenExchanges() {
    wireMock.register(
        WireMock.post("/oauth/token")
            .atPriority(1)
            .willReturn(WireMock.aResponse().withStatus(503).withBody("{}"))
    );
  }

  private static void awaitFailure(Future<?> future) throws Exception {
    var outcome = future
        .map(result -> (Object) result)
        .recover(Future::succeededFuture)
        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    assertTrue(outcome instanceof Throwable, "expected a failure, got " + outcome);
  }

  private static CCloudOAuthContext.RefreshFailures failures(int attempts, Instant firstFailedAt) {
    return new CCloudOAuthContext.RefreshFailures(attempts, firstFailedAt, firstFailedAt);
  }

  @Test
  void refreshShouldReturnFailedFutureIfTheRefreshTokenIsAbsent() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext
        .refresh(null)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Refresh token is missing.",
                      failure.getMessage());
                  assertEquals(
                      "io.confluent.idesidecar.restapi.exceptions."
                          + "CCloudAuthenticationFailedException",
                      failure.getClass().getCanonicalName()
                  );
                  // Error related to token refresh should be present
                  assertNotNull(authContext.getErrors().tokenRefresh());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void resetShouldVoidAllTokensAndErrors() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.succeeding(authenticatedAuthContext ->
                testContext.verify(() -> {
                  // Make sure that we got some tokens
                  assertNotNull(authContext.getControlPlaneToken());
                  assertNotNull(authContext.getDataPlaneToken());
                  // The auth context will expire at some point
                  assertTrue(authContext.expiresAt().isPresent());

                  authContext.reset();

                  // Neither errors nor failed token refresh attempts
                  assertEquals(new AuthErrors(), authContext.getErrors());
                  assertEquals(0, authContext.getFailedTokenRefreshAttempts());
                  // All tokens are gone
                  assertNull(authContext.getRefreshToken());
                  assertNull(authContext.getControlPlaneToken());
                  assertNull(authContext.getDataPlaneToken());
                  // The auth context will not expire because it does not hold any tokens
                  assertTrue(authContext.expiresAt().isEmpty());

                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void getSignInUriShouldReturnAValidUri() {
    var authContext = new CCloudOAuthContext();

    var signInUri = authContext.getSignInUri();

    assertTrue(isValidURL(signInUri));
    assertThat(signInUri, containsString("state=" + authContext.getOauthState()));
  }

  @Test
  void getUserEmailShouldReturnThePrincipalsEmailIfAuthenticated() throws Throwable {
    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.succeeding(authenticatedAuthContext ->
                testContext.verify(() -> {
                  // we know that the control plane token expires first
                  assertEquals(
                      "ssprenger+test@confluent.io",
                      authenticatedAuthContext.getUserEmail());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void getUserEmailShouldReturnPlaceholderIfNotAuthenticated() {
    var authContext = new CCloudOAuthContext();
    assertEquals("UNKNOWN", authContext.getUserEmail());
  }

  /**
   * Verifies that when {@code /oauth/token} returns 429 on the first call and the default
   * success response on the retry, the auth flow completes transparently. Exercises the full
   * {@link io.confluent.idesidecar.restapi.util.CCloudHttpSender#send} chain wired into the
   * auth path.
   */
  @Test
  void shouldRetryOnOAuthToken429AndSucceed() throws Throwable {
    // arrange: override /oauth/token to return 429 once, then advance scenario state so the
    // default low-priority stub serves the retry with the proper auth body
    wireMock.register(
        WireMock.post("/oauth/token")
            .inScenario("oauth-token-rate-limit")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(
                WireMock.aResponse()
                    .withStatus(429)
                    .withHeader("Retry-After", "1")
                    .withHeader("X-RateLimit-Limit", "10")
                    .withHeader("X-RateLimit-Remaining", "0")
                    .withHeader("X-RateLimit-Reset", "1")
                    .withBody("{\"error\":\"rate_limited\","
                        + "\"error_description\":\"too many requests\"}"))
            .willSetStateTo("retried")
            .atPriority(100)
    );

    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    // act + assert: auth completes despite the initial 429
    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.succeeding(authenticatedContext ->
                testContext.verify(() -> {
                  assertNotNull(authenticatedContext.getControlPlaneToken());
                  assertNotNull(authenticatedContext.getDataPlaneToken());
                  assertNull(authenticatedContext.getErrors().signIn());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(RATE_LIMIT_TIMEOUT_SEC, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  /**
   * Verifies that sustained 429 responses from {@code /oauth/token} surface as a failed Future
   * carrying {@link TooManyRequestsException} after retries are exhausted, rather than hanging
   * indefinitely or being swallowed.
   */
  @Test
  void shouldFailAuthAfterRetryExhaustionOn429() throws Throwable {
    // arrange: /oauth/token always returns 429 (high priority overrides the default success stub)
    wireMock.register(
        WireMock.post("/oauth/token")
            .willReturn(
                WireMock.aResponse()
                    .withStatus(429)
                    .withHeader("Retry-After", "1")
                    .withHeader("X-RateLimit-Limit", "10")
                    .withHeader("X-RateLimit-Remaining", "0")
                    .withHeader("X-RateLimit-Reset", "1")
                    .withBody("{\"error\":\"rate_limited\","
                        + "\"error_description\":\"too many requests\"}"))
            .atPriority(100)
    );

    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    // act + assert: future fails with TooManyRequestsException after retries exhaust
    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      TooManyRequestsException.class.getCanonicalName(),
                      failure.getClass().getCanonicalName());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(RATE_LIMIT_TIMEOUT_SEC, TimeUnit.SECONDS));
    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  boolean isValidURL(String url) {
    try {
      new URI(url).toURL();
      return true;
    } catch (MalformedURLException | URISyntaxException e) {
      return false;
    }
  }
}
