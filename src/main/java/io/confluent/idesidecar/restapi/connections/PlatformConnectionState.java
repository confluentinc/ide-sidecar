package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;

/**
 * Implementation of the connection state for Confluent Platform ({@link ConnectionType#PLATFORM}).
 */
public class PlatformConnectionState extends ConnectionState {

  public PlatformConnectionState() {
    super(null, null);
  }

  public PlatformConnectionState(
      @NotNull ConnectionSpec spec,
      @Nullable StateChangedListener listener
  ) {
    super(spec, listener);
  }
}
