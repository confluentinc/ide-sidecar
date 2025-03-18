package io.confluent.idesidecar.restapi.connections;

import static io.vertx.core.Future.succeededFuture;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.connections.ConnectionState.StateChangedListener;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.vertx.core.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetSystemProperty;

public class ConnectionStateTest {

  /**
   * We use a concrete implementation of {@link ConnectionState} to test the real implementations of
   * non-abstract methods. Since the tests cover methods that call to the listener, we need to
   * initialize the object properly, which requires an abstract class.
   *
   * <p>Unfortunately, using {@code mock(ConnectionState.class, Answers.CALLS_REAL_METHODS)} does
   * not work since Mockito does not call the constructor/initializer, which is necessary to set the
   * listener.
   */
  private static class TestableConnectionState extends ConnectionState {

    public TestableConnectionState(ConnectionSpec spec, StateChangedListener listener) {
      super(spec, listener);
    }

    @Override
    protected Future<ConnectionStatus> doRefreshStatus() {
      return super.doRefreshStatus();
    }

    @Override
    protected ConnectionStatus getInitialStatus() {
      return ATTEMPTING_STATUS;
    }
  }

  private static final ConnectionStatus INITIAL_STATUS = ConnectionStatus.INITIAL_STATUS;
  private static final ConnectionStatus ATTEMPTING_STATUS = INITIAL_STATUS.withKafkaCluster(
      new ConnectionStatus.KafkaClusterStatus(ConnectedState.ATTEMPTING, null, null)
  );
  private static final ConnectionStatus SUCCESS_STATUS = INITIAL_STATUS.withKafkaCluster(
      new ConnectionStatus.KafkaClusterStatus(ConnectedState.SUCCESS, null, null)
  );
  private static final ConnectionStatus FAILED_STATUS = INITIAL_STATUS.withKafkaCluster(
      new ConnectionStatus.KafkaClusterStatus(ConnectedState.FAILED, null, null)
  );

  private static final ConnectionSpec MOCK_SPEC = mock(ConnectionSpec.class);
  private ConnectionState connection;
  private StateChangedListener listener;

  @BeforeEach
  void beforeEach() {
    listener = mock(StateChangedListener.class);
    connection = spy(new TestableConnectionState(MOCK_SPEC, listener));
  }

  @Test
  void shouldNotifyListenerWithChangeInStatusToSuccess() {
    // When the refresh is computed, and the status changes from ATTEMPTING to SUCCESS
    when(connection.doRefreshStatus()).thenReturn(
        succeededFuture(SUCCESS_STATUS)
    );
    callAndWaitFor(connection.refreshStatus());

    // Then the listener should be notified once of the connection and nothing else
    verify(listener, times(1)).connected(connection);
    verifyNoMoreInteractions(listener);
  }

  @Test
  void shouldNotifyListenerWithChangeInStatusToFailed() {
    // When the refresh is computed, and the status changes from ATTEMPTING to FAILED
    when(connection.doRefreshStatus()).thenReturn(
        succeededFuture(FAILED_STATUS)
    );
    callAndWaitFor(connection.refreshStatus());

    // Then the listener should be notified once of the disconnection and nothing else
    verify(listener, times(1)).disconnected(connection);
    verifyNoMoreInteractions(listener);
  }

  @Test
  @SetSystemProperty(key = "ide-sidecar.websockets.broadcast-unchanged-updates", value = "false")
  void shouldNotNotifyListenerWithNoChangeInStatus() {
    // When the refresh is computed, and the status does not change
    when(connection.doRefreshStatus()).thenReturn(
        succeededFuture(ATTEMPTING_STATUS) // same as initial status
    );
    callAndWaitFor(connection.refreshStatus());

    // Then the listener should never be notified
    verifyNoInteractions(listener);
  }

  @Test
  @SetSystemProperty(key = "ide-sidecar.websockets.broadcast-unchanged-updates", value = "true")
  void shouldNotifyListenerWithNoChangeInStatusWithFlagSet() {
    // When the refresh is computed, and the status does not change
    when(connection.doRefreshStatus()).thenReturn(
        succeededFuture(ATTEMPTING_STATUS) // same as initial status
    );
    callAndWaitFor(connection.refreshStatus());

    // Then the listener should be notified once of the connection and nothing else
    verify(listener, times(1)).disconnected(connection);
    verifyNoMoreInteractions(listener);
  }


  @Test
  void shouldUseNoOpListenerWithNoChangeInStatus() {
    // Given a connection with no listener
    connection = spy(new TestableConnectionState(MOCK_SPEC, null));

    // When the refresh is computed, and the status DOES change
    when(connection.doRefreshStatus()).thenReturn(
        succeededFuture(FAILED_STATUS)
    );

    // Then this should not fail with an NPE since the listener is a no-op rather than null
    callAndWaitFor(connection.refreshStatus());
  }

  void callAndWaitFor(Future<ConnectionStatus> future) {
    future.toCompletionStage().toCompletableFuture().join();
  }
}
