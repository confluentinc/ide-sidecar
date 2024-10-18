package io.confluent.idesidecar.restapi.connections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext;
import io.confluent.idesidecar.restapi.auth.Token;
import io.confluent.idesidecar.restapi.connections.ConnectionState.StateChangedListener;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.Authentication.Status;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class CCloudConnectionTest {

  private static final int AWAIT_COMPLETION_TIMEOUT_SEC = 5;

  @Test
  void getConnectionStatusShouldReturnInitialStatusForCCloudConnectionsWithoutTokens()
      throws Throwable {
    var mockListener = mock(StateChangedListener.class);
    var testContext = new VertxTestContext();
    var connectionState = ConnectionStates.from(
        new ConnectionSpec("1", "foo", ConnectionType.CCLOUD),
        mockListener
    );

    connectionState.getConnectionStatus()
        .onComplete(
            testContext.succeeding(connectionStatus ->
                testContext.verify(() -> {
                  assertEquals(ConnectionStatus.INITIAL_STATUS, connectionStatus);
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void getConnectionStatusShouldReturnNoTokenForCCloudConnectionsIfRefreshTokenIsMissing()
      throws Throwable {

    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        true,
        false,
        true,
        true,
        false
    );
    connectionState.getConnectionStatus()
        .onComplete(
            testContext.succeeding(connectionStatus ->
                testContext.verify(() -> {
                  assertEquals(Status.NO_TOKEN, connectionStatus.authentication().status());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void getConnectionStatusShouldReturnNoTokenForCCloudConnectionsIfControlPlaneTokenIsMissing()
      throws Throwable {

    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        true,
        true,
        false,
        true,
        false
    );
    connectionState.getConnectionStatus()
        .onComplete(
            testContext.succeeding(connectionStatus ->
                testContext.verify(() -> {
                  assertEquals(Status.NO_TOKEN, connectionStatus.authentication().status());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void getConnectionStatusShouldReturnNoTokenForCCloudConnectionsIfDataPlaneTokenIsMissing()
      throws Throwable {

    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        true,
        true,
        true,
        false,
        false
    );
    connectionState.getConnectionStatus()
        .onComplete(
            testContext.succeeding(connectionStatus ->
                testContext.verify(() -> {
                  assertEquals(Status.NO_TOKEN, connectionStatus.authentication().status());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void getConnectionStatusShouldReturnValidTokenForCCloudConnectionsThatCanAuthenticate()
      throws Throwable {

    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        true,
        true,
        true,
        true,
        false
    );
    connectionState.getConnectionStatus()
        .onComplete(
            testContext.succeeding(connectionStatus ->
                testContext.verify(() -> {
                  assertEquals(Status.VALID_TOKEN, connectionStatus.authentication().status());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void getConnectionStatusShouldReturnInvalidTokenForCCloudConnectionsThatCannotAuthenticate()
      throws Throwable {

    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        false,
        true,
        true,
        true,
        false
    );
    connectionState.getConnectionStatus()
        .onComplete(
            testContext.succeeding(connectionStatus ->
                testContext.verify(() -> {
                  assertEquals(Status.INVALID_TOKEN, connectionStatus.authentication().status());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void getConnectionStatusShouldReturnFailedForCCloudConnectionsThatExperiencedNonTransientError()
      throws Throwable {

    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        false,
        true,
        true,
        true,
        true
    );
    connectionState.getConnectionStatus()
        .onComplete(
            testContext.succeeding(connectionStatus ->
                testContext.verify(() -> {
                  assertEquals(Status.FAILED, connectionStatus.authentication().status());
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void getInternalIdReturnsTheOauthStateParameter() {
    var connectionState = (CCloudConnectionState) ConnectionStates.from(
        new ConnectionSpec("1", "foo", ConnectionType.CCLOUD),
        null
    );

    assertEquals(
        connectionState.getOauthContext().getOauthState(),
        connectionState.getInternalId());
  }

  ConnectionState spyCCloudConnectionState(
      boolean authenticationStatus,
      boolean withRefreshToken,
      boolean withControlPlaneToken,
      boolean withDataPlaneToken,
      boolean withNonTransientError
  ) throws NoSuchFieldException, IllegalAccessException {

    var connectionState = spy(CCloudConnectionState.class);
    connectionState.setSpec(new ConnectionSpec("1", "foo", ConnectionType.CCLOUD));

    var authContext = mock(CCloudOAuthContext.class);
    when(authContext.checkAuthenticationStatus())
        .thenReturn(Future.succeededFuture(authenticationStatus));
    if (withRefreshToken) {
      when(authContext.getRefreshToken())
          .thenReturn(new Token("refresh-token", Instant.now()));
    }
    if (withControlPlaneToken) {
      when(authContext.getControlPlaneToken())
          .thenReturn(new Token("control-plane-token", Instant.now()));
    }
    if (withDataPlaneToken) {
      when(authContext.getDataPlaneToken())
          .thenReturn(new Token("data-plane-token", Instant.now()));
    }
    if (withNonTransientError) {
      when(authContext.hasNonTransientError()).thenReturn(true);
    }

    // Inject mocked CCloudOAuthContext into connection state via reflection
    var tokenField = connectionState.getClass().getDeclaredField("oauthContext");
    tokenField.setAccessible(true);
    tokenField.set(connectionState, authContext);

    return connectionState;
  }
}
