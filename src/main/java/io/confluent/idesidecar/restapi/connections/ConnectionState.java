package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.models.ConnectionMetadata;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.resources.ConnectionsResource;
import io.vertx.core.Future;

/**
 * Base class holding default implementations for interacting with connection states.
 *
 * @see CCloudConnectionState
 * @see LocalConnectionState
 * @see PlatformConnectionState
 */
public abstract class ConnectionState {

  /**
   * A listener for internal state changes on {@link ConnectionState} objects.
   */
  interface StateChangedListener {

    /**
     * A listener instance that does nothing.
     */
    StateChangedListener NO_OP = new StateChangedListener() {};

    /**
     * Signal that the supplied connection state has switched to connected.
     *
     * @param state the connection state that has changed
     */
    default void connected(ConnectionState state) {}

    /**
     * Signal that the supplied connection state has switched to disconnected.
     *
     * @param state the connection state that has changed
     */
    default void disconnected(ConnectionState state) {}
  }

  protected ConnectionSpec spec;

  protected final StateChangedListener listener;

  protected ConnectionState(ConnectionSpec spec, StateChangedListener listener) {
    this.spec = spec;
    this.listener = listener != null ? listener : StateChangedListener.NO_OP;
  }

  public ConnectionSpec getSpec() {
    return this.spec;
  }

  public ConnectionType getType() {
    return this.spec.type();
  }

  public void setSpec(ConnectionSpec in) {
    this.spec = in;
  }

  public Future<ConnectionStatus> getConnectionStatus() {
    return Future.succeededFuture(ConnectionStatus.INITIAL_STATUS);
  }

  public ConnectionMetadata getConnectionMetadata() {
    return new ConnectionMetadata(
        null,
        ConnectionsResource.API_RESOURCE_PATH,
        spec.id());
  }

  /**
   * Get the id from the {@link ConnectionSpec}.
   *
   * @return the {@link ConnectionState#spec}'s id or null if {@link ConnectionState#spec} is null
   */
  public String getId() {
    return spec != null ? spec.id() : null;
  }

  /**
   * Get the internal id of the {@link ConnectionState}, which must not be exposed externally. By
   * default, the internal id equals the {@link ConnectionSpec}'s id. This default implementation
   * can be changed when extending the {@link ConnectionState}.
   *
   * @return {@link ConnectionState#getId()}
   */
  public String getInternalId() {
    return getId();
  }
}