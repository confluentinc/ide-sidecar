package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.connections.ConnectionState.StateChangedListener;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;

/**
 * This class consists exclusively of static methods that operate on or return connection states.
 */
public final class ConnectionStates {

  private ConnectionStates() {
  }

  /**
   * Creates a {@link ConnectionState} from a {@link ConnectionSpec}. It uses the connection spec's
   * field type ,{@link ConnectionSpec#type()}, to determine the kind of connection to instantiate.
   *
   * @param spec     the connection spec.
   * @param listener the listener; may be null if no listener is required
   * @return A {@link CCloudConnectionState}, {@link LocalConnectionState}, or
   * {@link PlatformConnectionState}.
   */
  public static ConnectionState from(
      @NotNull ConnectionSpec spec,
      @Nullable StateChangedListener listener
  ) {
    return switch (spec.type()) {
      case CCLOUD -> new CCloudConnectionState(spec, listener);
      case LOCAL -> new LocalConnectionState(spec, listener);
      case DIRECT -> new DirectConnectionState(spec, listener);
      case PLATFORM -> new PlatformConnectionState(spec, listener);
    };
  }
}
