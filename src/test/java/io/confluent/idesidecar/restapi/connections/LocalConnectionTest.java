package io.confluent.idesidecar.restapi.connections;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.junit5.VertxTestContext;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class LocalConnectionTest {

  private static final int AWAIT_COMPLETION_TIMEOUT_SEC = 5;

  @Test
  void getConnectionStatusShouldReturnInitialStatusForLocalConnections() throws Throwable {
    var mockListener = mock(ConnectionState.StateChangedListener.class);
    var testContext = new VertxTestContext();
    var connectionState = ConnectionStates.from(
        new ConnectionSpec("1", "foo", ConnectionType.LOCAL),
        mockListener
    );

    connectionState.getConnectionStatus()
        .onComplete(
            testContext.succeeding(connectionStatus ->
                testContext.verify(() -> {
                  Assertions.assertEquals(ConnectionStatus.INITIAL_STATUS, connectionStatus);
                  testContext.completeNow();
                })));

    assertTrue(testContext.awaitCompletion(AWAIT_COMPLETION_TIMEOUT_SEC, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }
}
