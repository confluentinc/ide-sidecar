package io.confluent.idesidecar.restapi.auth;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Automates the refresh of CCloud tokens.
 */
@ApplicationScoped
public class RefreshCCloudTokensBean {

  private static final Integer MAX_TOKEN_REFRESH_ATTEMPTS = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.refresh-token.max-refresh-attempts", Integer.class);

  @Inject
  ConnectionStateManager connectionStateManager;

  /**
   * Recurring job that gets executed every
   * <code>ide-sidecar.connections.ccloud.check-token-expiration.interval-seconds</code> seconds.
   * Refreshes {@link CCloudOAuthContext}s of {@link CCloudConnectionState}s that hold
   * {@link Token}s that will expire before the next run of this recurring job. Resets
   * {@link CCloudOAuthContext}s that have experienced more than
   * <code>ide-sidecar.connections.ccloud.refresh-token.max-refresh-attempts</code>
   * failed token refresh attempts.
   */
  @Scheduled(
      every = "${ide-sidecar.connections.ccloud.check-token-expiration.interval-seconds}s",
      concurrentExecution = ConcurrentExecution.SKIP)
  void refreshTokens() {
    Supplier<Stream<CCloudConnectionState>> connectionsWithAuthContexts = () ->
        connectionStateManager
            .getConnectionStates().stream()
            // Filter for CCloud connection states and cast
            .filter(CCloudConnectionState.class::isInstance)
            .map(CCloudConnectionState.class::cast);

    // Refresh tokens
    connectionsWithAuthContexts.get()
        // Filter for auth contexts that must be refreshed
        .filter(connection -> connection.getOauthContext().shouldAttemptTokenRefresh())
        // Attempt token refresh
        .forEach(this::refreshAuthContext);

    // Reset auth contexts with too many failed token refresh attempts
    connectionsWithAuthContexts.get()
        .map(CCloudConnectionState::getOauthContext)
        .filter(authContext ->
            authContext.getFailedTokenRefreshAttempts() >= MAX_TOKEN_REFRESH_ATTEMPTS
        )
        .forEach(CCloudOAuthContext::reset);
  }

  /**
   * Refresh the {@link CCloudOAuthContext} of a given {@link CCloudConnectionState}.
   *
   * @param connectionState The connection that holds the auth context that should be refreshed.
   */
  void refreshAuthContext(CCloudConnectionState connectionState) {
    connectionState
        .getOauthContext()
        .refresh(connectionState.getSpec().ccloudOrganizationId())
        .onSuccess(result ->
            Log.infof(
                "Refreshed tokens of connection with ID=%s.",
                connectionState.getSpec().id()
            )
        )
        .onFailure(failure ->
            Log.errorf(
                "Refreshing tokens of connection with ID=%s failed: %s",
                connectionState.getSpec().id(),
                failure.getMessage()
            )
        );
  }

}
