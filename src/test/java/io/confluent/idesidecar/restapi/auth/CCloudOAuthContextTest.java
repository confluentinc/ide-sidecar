package io.confluent.idesidecar.restapi.auth;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.util.CCloud;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.junit5.VertxTestContext;
import jakarta.inject.Inject;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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

  WireMock wireMock;

  CCloudTestUtil ccloudTestUtil;

  private static final int AWAIT_COMPLETION_TIMEOUT_SEC = 5;

  private static final String FAKE_AUTHORIZATION_CODE = "fake_authorization_code";

  @BeforeEach
  void registerWireMockRoutes() {
    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
    ccloudTestUtil.registerWireMockRoutesForCCloudOAuth(
        FAKE_AUTHORIZATION_CODE, null, null);
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
  void createTokensFromAuthorizationCodeShouldReturnFailedFutureIfControlPlaneTokenExchangeFailed()
      throws Throwable {

    wireMock.register(
        WireMock
            .post("/api/sessions")
            .willReturn(
                WireMock
                    .aResponse()
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
                      "Could not parse the response from Confluent Cloud when exchanging "
                          + "the ID token for the control plane token.",
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
                    .withStatus(201)
                    .withBody("nope")
            ).atPriority(100));

    var testContext = new VertxTestContext();
    var authContext = new CCloudOAuthContext();

    authContext.createTokensFromAuthorizationCode(FAKE_AUTHORIZATION_CODE, null)
        .onComplete(
            testContext.failing(failure ->
                testContext.verify(() -> {
                  assertEquals(
                      "Could not parse the response from Confluent Cloud when retrieving "
                          + "the ID token.",
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

  boolean isValidURL(String url) {
    try {
      new URI(url).toURL();
      return true;
    } catch (MalformedURLException | URISyntaxException e) {
      return false;
    }
  }
}