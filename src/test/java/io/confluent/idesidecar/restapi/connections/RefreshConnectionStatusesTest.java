package io.confluent.idesidecar.restapi.connections;

import io.quarkus.test.junit.QuarkusTest;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
public class RefreshConnectionStatusesTest {

  CCloudConnectionState confluentCloudConnection = Mockito.mock(CCloudConnectionState.class);
  LocalConnectionState localConnection = Mockito.mock(LocalConnectionState.class);
  DirectConnectionState directConnection = Mockito.mock(DirectConnectionState.class);
  List<ConnectionState> mockedConnections = List.of(
      confluentCloudConnection,
      localConnection,
      directConnection
  );
  ConnectionStateManager connectionStateManager = Mockito.spy(ConnectionStateManager.class);
  RefreshConnectionStatuses refreshConnectionStatuses;

  @BeforeEach
  void setup() {
    Mockito.when(connectionStateManager.getConnectionStates()).thenReturn(mockedConnections);
    refreshConnectionStatuses = new RefreshConnectionStatuses(connectionStateManager);
  }

  @Test
  void refreshConfluentCloudConnectionStatusesShouldRefreshOnlyCCloudConnections() {
    refreshConnectionStatuses.refreshConfluentCloudConnectionStatuses();

    Mockito.verify(confluentCloudConnection, Mockito.times(1)).refreshStatus();
    Mockito.verify(localConnection, Mockito.never()).refreshStatus();
    Mockito.verify(directConnection, Mockito.never()).refreshStatus();
  }

  @Test
  void refreshLocalConnectionStatusesShouldRefreshOnlyLocalConnections() {
    refreshConnectionStatuses.refreshLocalConnectionStatuses();

    Mockito.verify(confluentCloudConnection, Mockito.never()).refreshStatus();
    Mockito.verify(localConnection, Mockito.times(1)).refreshStatus();
    Mockito.verify(directConnection, Mockito.never()).refreshStatus();
  }

  @Test
  void refreshDirectConnectionStatusesShouldRefreshOnlyDirectConnections() {
    refreshConnectionStatuses.refreshDirectConnectionStatuses();

    Mockito.verify(confluentCloudConnection, Mockito.never()).refreshStatus();
    Mockito.verify(localConnection, Mockito.never()).refreshStatus();
    Mockito.verify(directConnection, Mockito.times(1)).refreshStatus();
  }

  @Test
  void refreshConnectionStatusesShouldRefreshOnlyConnectionsMatchingProvidedPredicate() {
    // Refresh the statuses of only CCloud and Local connections
    refreshConnectionStatuses.refreshConnectionStatuses(connection ->
        connection instanceof CCloudConnectionState || connection instanceof LocalConnectionState
    );

    Mockito.verify(confluentCloudConnection, Mockito.times(1)).refreshStatus();
    Mockito.verify(localConnection, Mockito.times(1)).refreshStatus();
    Mockito.verify(directConnection, Mockito.never()).refreshStatus();
  }
}
