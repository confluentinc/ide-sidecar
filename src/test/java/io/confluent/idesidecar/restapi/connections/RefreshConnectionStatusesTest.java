package io.confluent.idesidecar.restapi.connections;

import io.quarkus.test.junit.QuarkusTest;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
public class RefreshConnectionStatusesTest {

  @Test
  void refreshConnectionStatusesShouldRefreshOnlyConnectionsMatchingProvidedPredicate() {
    var connectionStateManager = Mockito.spy(ConnectionStateManager.class);
    var confluentCloudConnection = Mockito.spy(CCloudConnectionState.class);
    var localConnection = Mockito.spy(LocalConnectionState.class);
    Mockito.when(connectionStateManager.getConnectionStates())
        .thenReturn(List.of(confluentCloudConnection, localConnection));

    var refreshConnectionStatuses = new RefreshConnectionStatuses(connectionStateManager);

    refreshConnectionStatuses.refreshConnectionStatuses(CCloudConnectionState.class::isInstance);

    Mockito.verify(confluentCloudConnection, Mockito.atLeastOnce()).refreshStatus();
    Mockito.verify(localConnection, Mockito.never()).refreshStatus();
  }
}
