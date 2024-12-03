package io.confluent.idesidecar.restapi.connections;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.Predicate;

/**
 * Bean that periodically refreshes the statuses of all connections managed by the
 * {@link ConnectionStateManager}.
 */
@ApplicationScoped
public class RefreshConnectionStatuses {

  final ConnectionStateManager connectionStateManager;

  @Inject
  RefreshConnectionStatuses(ConnectionStateManager connectionStateManager) {
    this.connectionStateManager = connectionStateManager;
  }

  @Scheduled(
      every = "${ide-sidecar.connections.ccloud.refresh-status-interval-seconds}s"
  )
  void refreshConfluentCloudConnectionStatuses() {
    refreshConnectionStatuses(CCloudConnectionState.class::isInstance);
  }

  @Scheduled(
      every = "${ide-sidecar.connections.confluent-local.refresh-status-interval-seconds}s"
  )
  void refreshLocalConnectionStatuses() {
    refreshConnectionStatuses(LocalConnectionState.class::isInstance);
  }

  @Scheduled(
      every = "${ide-sidecar.connections.direct.refresh-status-interval-seconds}s"
  )
  void refreshDirectConnectionStatuses() {
    refreshConnectionStatuses(DirectConnectionState.class::isInstance);
  }

  /**
   * Refreshes the statuses of all connections that match a given predicate.
   *
   * @param predicate The predicate that connections must match to be refreshed.
   */
  void refreshConnectionStatuses(Predicate<ConnectionState> predicate) {
    connectionStateManager.getConnectionStates().stream()
        .filter(predicate)
        .forEach(ConnectionState::refreshStatus);
  }
}
