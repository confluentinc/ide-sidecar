package io.confluent.idesidecar.restapi.connections;

import static io.confluent.idesidecar.restapi.connections.CCloudConnectionState.INITIAL_STATUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext;
import io.confluent.idesidecar.restapi.auth.Token;
import io.confluent.idesidecar.restapi.connections.ConnectionState.StateChangedListener;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.netty.channel.ConnectTimeoutException;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.NoStackTraceTimeoutException;
import io.vertx.junit5.VertxTestContext;
import java.net.ConnectException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class CCloudConnectionTest {

  private static final int AWAIT_COMPLETION_TIMEOUT_SEC = 5;
  private static final String CHECK_JWT_TIMEOUT = "The timeout period of 20000ms has been exceeded";
  private static final Duration HEALTHY_INTERVAL = Duration.ofSeconds(60);

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
  void refreshStatusShouldKeepLastStatusIfAuthStatusCheckTimesOut() throws Throwable {
    var connectionState = spyCCloudConnectionState(true, true, true, true, false, false);
    var authContext = ((CCloudConnectionState) connectionState).getOauthContext();
    var healthy = refreshStatus(connectionState);
    when(authContext.checkAuthenticationStatus()).thenReturn(Future.failedFuture(
        new CompletionException(new NoStackTraceTimeoutException(CHECK_JWT_TIMEOUT))
    ));

    var afterTimeout = refreshStatus(connectionState);

    assertEquals(ConnectedState.SUCCESS, healthy.ccloud().state());
    assertSame(healthy, afterTimeout);
    assertSame(healthy, connectionState.getStatus());
  }

  @Test
  void refreshStatusShouldKeepLastStatusIfAuthStatusCheckCannotConnectInTime() throws Throwable {
    var connectionState = spyCCloudConnectionState(true, true, true, true, false, false);
    var authContext = ((CCloudConnectionState) connectionState).getOauthContext();
    var healthy = refreshStatus(connectionState);
    when(authContext.checkAuthenticationStatus()).thenReturn(Future.failedFuture(
        new ConnectTimeoutException("connection timed out after 10000 ms")
    ));

    var afterTimeout = refreshStatus(connectionState);

    assertSame(healthy, afterTimeout);
  }

  @Test
  void refreshStatusShouldReturnExpiredIfAuthStatusCheckConnectionIsRefused() throws Throwable {
    var connectionState = spyCCloudConnectionState(true, true, true, true, false, false);
    var authContext = ((CCloudConnectionState) connectionState).getOauthContext();
    refreshStatus(connectionState);
    when(authContext.checkAuthenticationStatus())
        .thenReturn(Future.failedFuture(new ConnectException("Connection refused")));

    var status = refreshStatus(connectionState);

    assertEquals(ConnectedState.EXPIRED, status.ccloud().state());
  }

  @Test
  void refreshStatusShouldReturnExpiredIfFirstAuthStatusCheckTimesOut() throws Throwable {
    var connectionState = spyCCloudConnectionState(true, true, true, true, false, false);
    var authContext = ((CCloudConnectionState) connectionState).getOauthContext();
    when(authContext.checkAuthenticationStatus())
        .thenReturn(Future.failedFuture(new NoStackTraceTimeoutException(CHECK_JWT_TIMEOUT)));

    var status = refreshStatus(connectionState);

    assertEquals(ConnectedState.EXPIRED, status.ccloud().state());
  }

  @Test
  void refreshStatusShouldReturnExpiredIfAuthStatusCheckFailsWithoutTimeout() throws Throwable {
    var connectionState = spyCCloudConnectionState(true, true, true, true, false, false);
    var authContext = ((CCloudConnectionState) connectionState).getOauthContext();
    refreshStatus(connectionState);
    when(authContext.checkAuthenticationStatus())
        .thenReturn(Future.failedFuture(new IllegalStateException("connection reset")));

    var status = refreshStatus(connectionState);

    assertEquals(ConnectedState.EXPIRED, status.ccloud().state());
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

  private static ConnectionStatus refreshStatus(ConnectionState connectionState)
      throws Exception {
    return connectionState.refreshStatus()
        .toCompletionStage()
        .toCompletableFuture()
        .get(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS);
  }

  @Test
  void shouldNotCheckJwtAgainWhileTheConnectionIsHealthy() throws Exception {
    var connectionState = signedInConnectionState(Future.succeededFuture(true));

    var first = awaitStatus(connectionState.refreshStatus());
    var second = awaitStatus(connectionState.refreshStatus());

    assertEquals(ConnectedState.SUCCESS, first.ccloud().state());
    assertEquals(ConnectedState.SUCCESS, second.ccloud().state());
    verify(connectionState.getOauthContext(), times(1)).checkAuthenticationStatus();
  }

  @Test
  void shouldUseTheLastHealthyCheckUntilTheIntervalEnds() throws Exception {
    // arrange
    var connectionState = signedInConnectionState(Future.succeededFuture(true));
    var start = Instant.now();
    doReturn(start).when(connectionState).now();
    awaitStatus(connectionState.refreshStatus());
    var interval = HEALTHY_INTERVAL;
    doReturn(start.plus(interval).minusMillis(1)).when(connectionState).now();

    // act
    awaitStatus(connectionState.refreshStatus());

    // assert: the jitter only ever lengthens the interval
    verify(connectionState.getOauthContext(), times(1)).checkAuthenticationStatus();
  }

  @Test
  void shouldCheckJwtAgainOnceTheHealthyIntervalHasPassed() throws Exception {
    // arrange
    var connectionState = signedInConnectionState(Future.succeededFuture(true));
    var start = Instant.now();
    doReturn(start).when(connectionState).now();
    awaitStatus(connectionState.refreshStatus());
    var longestInterval = HEALTHY_INTERVAL.multipliedBy(2);
    doReturn(start.plus(longestInterval)).when(connectionState).now();

    // act
    awaitStatus(connectionState.refreshStatus());

    // assert
    verify(connectionState.getOauthContext(), times(2)).checkAuthenticationStatus();
  }

  @Test
  void shouldCountTheHealthyIntervalFromWhenTheCheckWasSent() throws Exception {
    // arrange: a check sent at the start that answers 20s later
    var slowCheck = Promise.<Boolean>promise();
    var connectionState = signedInConnectionState(slowCheck.future());
    var start = Instant.now();
    doReturn(start).when(connectionState).now();
    var firstRefresh = connectionState.refreshStatus();
    doReturn(start.plusSeconds(20)).when(connectionState).now();
    slowCheck.complete(true);
    awaitStatus(firstRefresh);
    var longestInterval = Duration.ofMillis(Math.round(
        HEALTHY_INTERVAL.toMillis() * (1 + CCloudConnectionState.CHECK_JWT_JITTER_FACTOR)
    ));
    doReturn(start.plus(longestInterval)).when(connectionState).now();

    // act
    awaitStatus(connectionState.refreshStatus());

    // assert
    verify(connectionState.getOauthContext(), times(2)).checkAuthenticationStatus();
  }

  @Test
  void shouldKeepTheStatusButCheckAgainAfterACheckJwtTimesOut() throws Exception {
    // arrange
    var connectionState = signedInConnectionState(Future.succeededFuture(true));
    var start = Instant.now();
    doReturn(start).when(connectionState).now();
    var healthy = awaitStatus(connectionState.refreshStatus());
    when(connectionState.getOauthContext().checkAuthenticationStatus()).thenReturn(
        Future.failedFuture(new NoStackTraceTimeoutException(CHECK_JWT_TIMEOUT)),
        Future.succeededFuture(true)
    );
    doReturn(start.plus(HEALTHY_INTERVAL.multipliedBy(2))).when(connectionState).now();

    // act
    var afterTimeout = awaitStatus(connectionState.refreshStatus());
    awaitStatus(connectionState.refreshStatus());

    // assert: a timeout isn't cached as healthy, so the next refresh checks again
    assertSame(healthy, afterTimeout);
    verify(connectionState.getOauthContext(), times(3)).checkAuthenticationStatus();
  }

  @Test
  void shouldCheckJwtAgainOnceTheControlPlaneTokenExpires() throws Exception {
    // arrange
    var connectionState = signedInConnectionState(Future.succeededFuture(true));
    awaitStatus(connectionState.refreshStatus());
    var expiresAt = connectionState.getOauthContext().getControlPlaneToken().expiresAt();
    doReturn(expiresAt).when(connectionState).now();

    // act
    awaitStatus(connectionState.refreshStatus());

    // assert
    verify(connectionState.getOauthContext(), times(2)).checkAuthenticationStatus();
  }

  @Test
  void shouldKeepCheckingAfterAHealthyConnectionExpires() throws Exception {
    // arrange: healthy, then the interval passes and the session is gone
    var connectionState = signedInConnectionState(Future.succeededFuture(true));
    var start = Instant.now();
    doReturn(start).when(connectionState).now();
    awaitStatus(connectionState.refreshStatus());
    var later = start.plus(HEALTHY_INTERVAL.multipliedBy(2));
    doReturn(later).when(connectionState).now();
    when(connectionState.getOauthContext().checkAuthenticationStatus())
        .thenReturn(Future.succeededFuture(false));

    // act
    var expired = awaitStatus(connectionState.refreshStatus());
    awaitStatus(connectionState.refreshStatus());

    // assert
    assertEquals(ConnectedState.EXPIRED, expired.ccloud().state());
    verify(connectionState.getOauthContext(), times(3)).checkAuthenticationStatus();
  }

  @Test
  void shouldTreatACheckJwtThatThrowsAsExpired() throws Exception {
    // arrange
    var connectionState = signedInConnectionState(Future.succeededFuture(true));
    when(connectionState.getOauthContext().checkAuthenticationStatus())
        .thenThrow(new IllegalStateException("boom"));

    // act
    var status = awaitStatus(connectionState.refreshStatus());

    // assert: and the next refresh can still check
    assertEquals(ConnectedState.EXPIRED, status.ccloud().state());
    awaitStatus(connectionState.refreshStatus());
    verify(connectionState.getOauthContext(), times(2)).checkAuthenticationStatus();
  }

  @Test
  void shouldNotJoinACheckJwtForAnOlderToken() throws Exception {
    // arrange: a check for the first token that never finishes
    Promise<Boolean> hung = Promise.promise();
    var connectionState = signedInConnectionState(hung.future());
    connectionState.refreshStatus();
    when(connectionState.getOauthContext().getControlPlaneToken())
        .thenReturn(new Token("signed-in-again", Instant.now().plusSeconds(300)));
    when(connectionState.getOauthContext().checkAuthenticationStatus())
        .thenReturn(Future.succeededFuture(true));

    // act
    var status = awaitStatus(connectionState.refreshStatus());

    // assert
    assertEquals(ConnectedState.SUCCESS, status.ccloud().state());
  }

  @Test
  void shouldCheckJwtEveryTickWhileTheConnectionIsExpired() throws Exception {
    var connectionState = signedInConnectionState(Future.succeededFuture(false));

    awaitStatus(connectionState.refreshStatus());
    awaitStatus(connectionState.refreshStatus());

    verify(connectionState.getOauthContext(), times(2)).checkAuthenticationStatus();
  }

  @Test
  void shouldCheckJwtAgainWhenTheControlPlaneTokenChanges() throws Exception {
    var connectionState = signedInConnectionState(Future.succeededFuture(true));
    awaitStatus(connectionState.refreshStatus());

    when(connectionState.getOauthContext().getControlPlaneToken())
        .thenReturn(new Token("refreshed-control-plane-token", Instant.now().plusSeconds(300)));
    awaitStatus(connectionState.refreshStatus());

    verify(connectionState.getOauthContext(), times(2)).checkAuthenticationStatus();
  }

  @Test
  void shouldJoinACheckJwtThatIsStillRunning() throws Exception {
    Promise<Boolean> stillRunning = Promise.promise();
    var connectionState = signedInConnectionState(stillRunning.future());

    var first = connectionState.refreshStatus();
    var second = connectionState.refreshStatus();
    stillRunning.complete(true);

    assertEquals(ConnectedState.SUCCESS, awaitStatus(first).ccloud().state());
    assertEquals(ConnectedState.SUCCESS, awaitStatus(second).ccloud().state());
    verify(connectionState.getOauthContext(), times(1)).checkAuthenticationStatus();
  }

  /**
   * A connection with all three tokens, valid for the next 5 minutes, whose {@code check_jwt}
   * returns {@code checkResult}.
   */
  private CCloudConnectionState signedInConnectionState(Future<Boolean> checkResult)
      throws NoSuchFieldException, IllegalAccessException {
    var connectionState = spy(CCloudConnectionState.class);
    connectionState.setSpec(new ConnectionSpec("1", "foo", ConnectionType.CCLOUD));
    var authContext = mock(CCloudOAuthContext.class);
    var expiresAt = Instant.now().plusSeconds(300);
    when(authContext.checkAuthenticationStatus()).thenReturn(checkResult);
    when(authContext.getRefreshToken()).thenReturn(new Token("refresh-token", expiresAt));
    when(authContext.getControlPlaneToken())
        .thenReturn(new Token("control-plane-token", expiresAt));
    when(authContext.getDataPlaneToken()).thenReturn(new Token("data-plane-token", expiresAt));
    var field = CCloudConnectionState.class.getDeclaredField("oauthContext");
    field.setAccessible(true);
    field.set(connectionState, authContext);
    // the test profile turns the interval off; these tests need the production behavior
    doReturn(HEALTHY_INTERVAL).when(connectionState).checkJwtHealthyInterval();
    return connectionState;
  }

  private static ConnectionStatus awaitStatus(Future<ConnectionStatus> status) throws Exception {
    return status.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
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
