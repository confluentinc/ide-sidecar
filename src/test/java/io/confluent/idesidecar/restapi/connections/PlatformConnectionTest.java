package io.confluent.idesidecar.restapi.connections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.junit5.VertxTestContext;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class PlatformConnectionTest {

  @Test
  void getConnectionStatusShouldReturnInitialStatusForConfluentPlatformConnections() {
    var mockListener = mock(ConnectionState.StateChangedListener.class);
    var connectionState = ConnectionStates.from(
        new ConnectionSpec("1", "foo", ConnectionType.PLATFORM),
        mockListener
    );

    assertEquals(ConnectionStatus.INITIAL_STATUS, connectionState.getStatus());
  }
}
