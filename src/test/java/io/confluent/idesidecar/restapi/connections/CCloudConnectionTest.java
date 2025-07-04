package io.confluent.idesidecar.restapi.connections;

import static io.confluent.idesidecar.restapi.connections.CCloudConnectionState.INITIAL_STATUS;
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
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
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
  void getStatusShouldReturnInitialStatusForNewConnections() {
    var mockListener = mock(StateChangedListener.class);
    var connectionState = ConnectionStates.from(
        new ConnectionSpec("1", "foo", ConnectionType.CCLOUD),
        mockListener
    );
    assertEquals(ConnectionStatus.INITIAL_CCLOUD_STATUS, connectionState.getStatus());
  }

  @Test
  void refreshStatusShouldReturnInitialStatusForConnectionsWithoutTokens() throws Throwable {
    var mockListener = mock(StateChangedListener.class);
    var connectionState = ConnectionStates.from(
        new ConnectionSpec("1", "foo", ConnectionType.CCLOUD),
        mockListener
    );

    var testContext = new VertxTestContext();
    connectionState.refreshStatus()
        .onComplete(
            testContext.succeeding(status ->
                testContext.verify(() -> {
                  assertEquals(
                      ConnectionStatus.INITIAL_CCLOUD_STATUS,
                      status
                  );
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void refreshStatusShouldReturnNoneIfRefreshTokenIsMissing() throws Throwable {
    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        true,
        false,
        true,
        true,
        false,
        false
    );
    connectionState.refreshStatus()
        .onComplete(
            testContext.succeeding(status ->
                testContext.verify(() -> {
                  assertEquals(
                      ConnectedState.NONE,
                      status.ccloud().state()
                  );
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void refreshStatusShouldReturnNoneIfControlPlaneTokenIsMissing() throws Throwable {
    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        true,
        true,
        false,
        true,
        false,
        false
    );
    connectionState.refreshStatus()
        .onComplete(
            testContext.succeeding(status ->
                testContext.verify(() -> {
                  assertEquals(
                      ConnectedState.NONE,
                      status.ccloud().state()
                  );
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void refreshStatusShouldReturnNoneIfDataPlaneTokenIsMissing() throws Throwable {
    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        true,
        true,
        true,
        false,
        false,
        false
    );
    connectionState.refreshStatus()
        .onComplete(
            testContext.succeeding(status ->
                testContext.verify(() -> {
                  assertEquals(
                      ConnectedState.NONE,
                      status.ccloud().state()
                  );
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void refreshStatusShouldReturnInitialStatusIfEndOfLifetimeIsReached() throws Throwable {
    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        true,
        true,
        true,
        true,
        false,
        true
    );

    connectionState.refreshStatus()
        .onComplete(
            testContext.succeeding(status ->
                testContext.verify(() -> {
                  assertEquals(
                      INITIAL_STATUS.ccloud(),
                      status.ccloud()
                  );
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void refreshStatusShouldReturnSuccessIfAuthenticationSucceeds() throws Throwable {
    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        true,
        true,
        true,
        true,
        false,
        false
    );
    connectionState.refreshStatus()
        .onComplete(
            testContext.succeeding(status ->
                testContext.verify(() -> {
                  assertEquals(
                      ConnectedState.SUCCESS,
                      status.ccloud().state()
                  );
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void refreshStatusShouldReturnExpiredIfAuthenticationFails() throws Throwable {
    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        false,
        true,
        true,
        true,
        false,
        false
    );
    connectionState.refreshStatus()
        .onComplete(
            testContext.succeeding(status ->
                testContext.verify(() -> {
                  assertEquals(
                      ConnectedState.EXPIRED,
                      status.ccloud().state()
                  );
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }

  @Test
  void refreshStatusShouldReturnFailedForConnectionsWithNonTransientError() throws Throwable {
    var testContext = new VertxTestContext();
    var connectionState = spyCCloudConnectionState(
        false,
        true,
        true,
        true,
        true,
        false
    );
    connectionState.refreshStatus()
        .onComplete(
            testContext.succeeding(status ->
                testContext.verify(() -> {
                  assertEquals(
                      ConnectedState.FAILED,
                      status.ccloud().state()
                  );
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
      boolean withNonTransientError,
      boolean withReachedEndOfLifetime
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
    if (withReachedEndOfLifetime) {
      when(authContext.hasReachedEndOfLifetime()).thenReturn(true);
    }

    // Inject mocked CCloudOAuthContext into connection state via reflection
    var tokenField = connectionState.getClass().getDeclaredField("oauthContext");
    tokenField.setAccessible(true);
    tokenField.set(connectionState, authContext);

    return connectionState;
  }
}
