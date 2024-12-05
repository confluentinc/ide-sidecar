package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.auth.AuthContext;
import io.confluent.idesidecar.restapi.auth.MdsAuthContext;
import io.confluent.idesidecar.restapi.credentials.Credentials;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of the connection state for Confluent Platform ({@link ConnectionType#PLATFORM}).
 */
public class PlatformConnectionState extends ConnectionState {

  static ConnectionStatus mdsStatusWith(@NotNull ConnectedState state) {
    return new ConnectionStatus(
        null,
        null,
        null,
        new ConnectionStatus.MdsStatus(
            state,
            null,
            null
        )
    );
  }

  static final ConnectionStatus INITIAL_STATUS_WITHOUT_MDS = new ConnectionStatus(
      null,
      null,
      null,
      null
  );


  private final AtomicReference<MdsAuthContext> mdsAuthContext = new AtomicReference<>();

  public PlatformConnectionState(
      @NotNull ConnectionSpec spec,
      @Nullable StateChangedListener listener
  ) {
    super(spec, listener);
    setAuthContext(spec);
  }

  protected void setAuthContext(ConnectionSpec spec) {
    var mds = spec.mdsConfig();
    mdsAuthContext.set(mds != null ? new MdsAuthContext(mds) : null);
  }

  @Override
  protected ConnectionStatus getInitialStatus() {
    return spec.mdsConfig() != null
           ? mdsStatusWith(ConnectedState.ATTEMPTING)
           : INITIAL_STATUS_WITHOUT_MDS;
  }

  /**
   * Called when the spec for the connection has potentially changed. This not only updates
   * the reference to the spec, but also updates the internal {@link #mdsAuthContext auth context}
   * used by this connection state.
   *
   * @param spec the new spec for the connection
   *
   * @see ConnectionStateManager#updateSpecForConnectionState(String, ConnectionSpec)
   */
  @Override
  public void setSpec(ConnectionSpec spec) {
    super.setSpec(spec);
    setAuthContext(spec);
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

  public MultiMap getAuthenticationHeaders() {
    var headers = HttpHeaders.headers();
    var mds = spec.mdsConfig();
    if (mds != null && mds.credentials() != null) {
      mds.credentials()
         .httpClientHeaders()
         .ifPresent(map -> map.forEach(headers::add));
    }
    return headers;
  }

  public Optional<Credentials> getCredentials() {
    var mds = spec.mdsConfig();
    var creds = mds != null && mds.credentials() != null ? mds.credentials() : null;
    return Optional.ofNullable(creds);
  }

  @Override
  public Optional<Credentials> getKafkaCredentials() {
    // TODO: If we need different credentials for specific clusters, we might want to track
    //       additional credentials for specific clusters.
    return getCredentials();
  }

  @Override
  public Optional<Credentials> getSchemaRegistryCredentials() {
    return getCredentials();
  }

  @Override
  protected Future<ConnectionStatus> doRefreshStatus() {
    var authContext = mdsAuthContext.get();
    if (authContext == null) {
      return Future.succeededFuture(INITIAL_STATUS_WITHOUT_MDS);
    }
    return authContext
        .maybeRefresh()
        .compose(AuthContext::checkAuthenticationStatus)
        .map(success -> success ? ConnectedState.SUCCESS : ConnectedState.FAILED)
        .map(PlatformConnectionState::mdsStatusWith);
  }
}
