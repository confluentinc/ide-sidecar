package io.confluent.idesidecar.restapi.auth;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.connections.LocalConnectionState;
import io.confluent.idesidecar.restapi.connections.PlatformConnectionState;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.core.Future;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
class RefreshCCloudTokensBeanTest {
  @InjectMock
  ConnectionStateManager connectionStateManager;

  @Inject
  RefreshCCloudTokensBean refreshCCloudTokensBean;

  @Test
  void refreshTokensShouldConsiderOnlyCCloudConnections() {
    var confluentCloudConnection = (CCloudConnectionState) createdSpiedConnectionState(
        "1", "1", ConnectionType.CCLOUD);
    var localConnection = createdSpiedConnectionState("2", "2", ConnectionType.LOCAL);
    var connections = List.of(
        confluentCloudConnection,
        localConnection
    );
    Mockito.when(connectionStateManager.getConnectionStates()).thenReturn(connections);

    refreshCCloudTokensBean.refreshTokens();

    Mockito.verify(confluentCloudConnection, Mockito.atLeastOnce()).getOauthContext();
  }

  @Test
  void refreshTokensShouldRefreshOnlyConnectionsEligibleForATokenRefreshAttempt() {
    // Connection eligible for a token refresh attempt
    var eligibleConnection = (CCloudConnectionState) createdSpiedConnectionState(
        "eligible", "name", ConnectionType.CCLOUD);
    var eligibleAuthContext = createSpiedCCloudOAuthContext(
        true,
        // No failed token refresh attempts
        0,
        // Tokens will expire before next run in 10 seconds
        Optional.of(Instant.now().plusSeconds(5)),
        Optional.of(Instant.now().plusSeconds(15))
    );
    Mockito.when(eligibleConnection.getOauthContext()).thenReturn(eligibleAuthContext);


    // Connection not eligible for a token refresh attempt
    var ineligibleConnection = (CCloudConnectionState) createdSpiedConnectionState(
        "ineligible", "name", ConnectionType.CCLOUD);
    var ineligibleAuthContext = createSpiedCCloudOAuthContext(
        true,
        // Too many failed token refresh attempts
        50,
        // Tokens will expire before next run in 10 seconds
        Optional.of(Instant.now().plusSeconds(5)),
        Optional.of(Instant.now().plusSeconds(15))
    );
    Mockito.when(ineligibleConnection.getOauthContext()).thenReturn(ineligibleAuthContext);

    var localConnection = createdSpiedConnectionState(
        "local", "name", ConnectionType.LOCAL);

    var connections = List.of(eligibleConnection, ineligibleConnection, localConnection);
    Mockito.when(connectionStateManager.getConnectionStates()).thenReturn(connections);

    refreshCCloudTokensBean.refreshTokens();

    // Should attempt a token refresh for only the eligible auth context
    Mockito.verify(eligibleAuthContext, Mockito.atLeastOnce()).refresh(null);
    Mockito.verify(ineligibleAuthContext, Mockito.never()).refresh(null);
  }

  @Test
  void refreshTokensShouldResetAuthContextsWithTooManyFailedAttempts() {
    // Connection eligible for a token refresh attempt
    var eligibleConnection = (CCloudConnectionState) createdSpiedConnectionState(
        "eligible", "name", ConnectionType.CCLOUD);
    var eligibleAuthContext = createSpiedCCloudOAuthContext(
        true,
        // No failed token refresh attempts
        0,
        // Tokens will expire before next run in 10 seconds
        Optional.of(Instant.now().plusSeconds(5)),
        Optional.of(Instant.now().plusSeconds(15))
    );
    Mockito.when(eligibleConnection.getOauthContext()).thenReturn(eligibleAuthContext);

    // Connection not eligible for a token refresh attempt
    var ineligibleConnection = (CCloudConnectionState) createdSpiedConnectionState(
        "ineligible", "name", ConnectionType.CCLOUD);
    var ineligibleAuthContext = createSpiedCCloudOAuthContext(
        true,
        // Too many failed token refresh attempts
        50,
        // Tokens will expire before next run in 10 seconds
        Optional.of(Instant.now().plusSeconds(5)),
        Optional.of(Instant.now().plusSeconds(15))
    );
    Mockito.when(ineligibleConnection.getOauthContext()).thenReturn(ineligibleAuthContext);

    var localConnection = createdSpiedConnectionState(
        "local", "name", ConnectionType.LOCAL);

    var connections = List.of(eligibleConnection, ineligibleConnection, localConnection);
    Mockito.when(connectionStateManager.getConnectionStates()).thenReturn(connections);

    refreshCCloudTokensBean.refreshTokens();

    // Should attempt a token refresh for only the eligible auth context
    Mockito.verify(eligibleAuthContext, Mockito.atLeastOnce()).refresh(null);
    Mockito.verify(ineligibleAuthContext, Mockito.never()).refresh(null);
    Mockito.verify(eligibleAuthContext, Mockito.never()).reset();
    Mockito.verify(ineligibleAuthContext, Mockito.atLeastOnce()).reset();
  }

  private ConnectionState createdSpiedConnectionState(String id,
                                                      String name,
                                                      ConnectionType connectionType) {
    var clazz = switch (connectionType) {
      case CCLOUD   -> CCloudConnectionState.class;
      case LOCAL    -> LocalConnectionState.class;
      case PLATFORM -> PlatformConnectionState.class;
    };

    var connectionState = Mockito.spy(clazz);
    connectionState.setSpec(new ConnectionSpec(id, name, connectionType));

    return connectionState;
  }

  private CCloudOAuthContext createSpiedCCloudOAuthContext(boolean canRefresh,
                                                           Integer failedTokenRefreshAttempts,
                                                           Optional<Instant> expiresAt,
                                                           Optional<Instant> endOfLifetime) {
    var authContext = Mockito.spy(CCloudOAuthContext.class);

    Future<AuthContext> resultOfRefresh = canRefresh
        ? Future.succeededFuture(authContext)
        : Future.failedFuture(new Throwable("Refreshing failed"));
    Mockito.when(authContext.refreshIgnoreFailures(null)).thenReturn(resultOfRefresh);
    Mockito.when(authContext.getFailedTokenRefreshAttempts()).thenReturn(
        failedTokenRefreshAttempts);

    if (expiresAt.isPresent()) {
      Mockito.when(authContext.expiresAt()).thenReturn(expiresAt);
    }

    endOfLifetime.ifPresent(
        instant -> Mockito.when(authContext.getEndOfLifetime()).thenReturn(instant));

    return authContext;
  }
}