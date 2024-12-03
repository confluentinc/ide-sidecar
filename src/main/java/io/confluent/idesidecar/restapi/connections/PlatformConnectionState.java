package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;

/**
 * Implementation of the connection state for Confluent Platform ({@link ConnectionType#PLATFORM}).
 */
public class PlatformConnectionState extends ConnectionState {

  static final ConnectionStatus.MdsStatus MDS_INITIAL_STATUS = new ConnectionStatus.MdsStatus(
      ConnectionStatus.ConnectedState.ATTEMPTING,
      null,
      null
  );

  public PlatformConnectionState() {
    super(null, null);
  }

  public PlatformConnectionState(
      @NotNull ConnectionSpec spec,
      @Nullable StateChangedListener listener
  ) {
    super(spec, listener);
  }

  @Override
  protected ConnectionStatus getInitialStatus() {
    return new ConnectionStatus(
        null,
        null,
        null,
        spec.mdsConfig() != null ? MDS_INITIAL_STATUS : null
    );
  }

  /**
   * Check if the connection to MDS is currently connected.
   *
   * @return true if the MDS component is connected
   */
  public boolean isMdsConnected() {
    var status = getStatus();
    return status.mds() != null && status.mds().isConnected();
  }

  public MultiMap getAuthenticationHeaders(ClusterType clusterType) {
    var headers = HttpHeaders.headers();
    var mds = spec.mdsConfig();
    if (mds != null && mds.credentials() != null) {
      mds.credentials()
         .httpClientHeaders()
         .ifPresent(map -> map.forEach(headers::add));
    }
    return headers;
  }
}
