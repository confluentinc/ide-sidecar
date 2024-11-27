package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;

/**
 * Implementation of the connection state for Confluent Local ({@link ConnectionType#LOCAL}).
 */
public class LocalConnectionState extends ConnectionState {

  public LocalConnectionState() {
    super(null, null);
  }

  public LocalConnectionState(
      @NotNull ConnectionSpec spec,
      @Nullable StateChangedListener listener
  ) {
    super(spec, listener);
    // Notify the never-null listener of successful connection, even though no auth is used
    this.listener.connected(this);
  }
}
