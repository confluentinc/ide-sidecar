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

/**
 * Bean that periodically checks the status of Confluent Cloud API tokens and refreshes them if they
 * are about to expire.
 */
@ApplicationScoped
public class RefreshCCloudTokensBean {

  final ConnectionStateManager connectionStateManager;

  @Inject
  RefreshCCloudTokensBean(ConnectionStateManager connectionStateManager) {
    this.connectionStateManager = connectionStateManager;
  }

  /**
   * Recurring job that gets executed every
   * <code>ide-sidecar.connections.ccloud.check-token-expiration.interval-seconds</code> seconds.
   * Refreshes {@link CCloudOAuthContext}s of {@link CCloudConnectionState}s that hold
   * {@link Token}s that will expire before the next run of this recurring job, skipping those
   * still backing off after a failed refresh (see
   * {@link CCloudOAuthContext#shouldAttemptTokenRefresh()}).
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
                failure,
                "Refreshing tokens of connection with ID=%s failed (%d in a row): %s",
                connectionState.getSpec().id(),
                connectionState.getOauthContext().getFailedTokenRefreshAttempts(),
                failure.getMessage()
            )
        );
  }

}
