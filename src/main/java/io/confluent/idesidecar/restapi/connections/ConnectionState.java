package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.credentials.Credentials;
import io.confluent.idesidecar.restapi.credentials.Credentials.KafkaConnectionOptions;
import io.confluent.idesidecar.restapi.credentials.CredentialsKafkaConnectionOptionsBuilder;
import io.confluent.idesidecar.restapi.credentials.CredentialsSchemaRegistryConnectionOptionsBuilder;
import io.confluent.idesidecar.restapi.credentials.TLSConfig;
import io.confluent.idesidecar.restapi.models.ConnectionMetadata;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.resources.ConnectionsResource;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class holding default implementations for interacting with connection states.
 *
 * @see CCloudConnectionState
 * @see LocalConnectionState
 */
public abstract class ConnectionState {

  /**
   * A listener for internal state changes on {@link ConnectionState} objects.
   */
  interface StateChangedListener {

    /**
     * A listener instance that does nothing.
     */
    StateChangedListener NO_OP = new StateChangedListener() {
    };

    /**
     * Signal that the supplied connection state has switched to connected.
     *
     * @param state the connection state that has changed
     */
    default void connected(ConnectionState state) {
    }

    /**
     * Signal that the supplied connection state has switched to disconnected.
     *
     * @param state the connection state that has changed
     */
    default void disconnected(ConnectionState state) {
    }
  }

  protected ConnectionSpec spec;

  private final AtomicReference<ConnectionStatus> cachedStatus = new AtomicReference<>();

  /**
   * The instant at which the cached status was last updated. This is used to prevent stale status
   * updates from being applied by the scheduled refresh task (in
   * {@link RefreshConnectionStatuses}).
   */
  private final AtomicReference<Instant> lastUpdated = new AtomicReference<>(
      Instant.now()
  );

  private final StateChangedListener listener;

  protected ConnectionState(ConnectionSpec spec, StateChangedListener listener) {
    this.spec = spec;
    this.listener = listener != null ? listener : StateChangedListener.NO_OP;
    this.cachedStatus.set(getInitialStatus());
  }

  public ConnectionSpec getSpec() {
    return this.spec;
  }

  public ConnectionType getType() {
    return this.spec.type();
  }

  public void setSpec(ConnectionSpec in) {
    this.spec = in;

    // Reset the status to the initial status when the spec changes
    updateStatus(this.cachedStatus.get(), getInitialStatus());
  }

  /**
   * Obtain the most recently-obtained status of the connection.
   *
   * @return the connection status; never null
   */
  public ConnectionStatus getStatus() {
    return this.cachedStatus.get();
  }

  /**
   * Return the status for a newly-created connection. This method can be overridden by subclasses
   * to provide a different initial status.
   *
   * @return the initial status; never null
   */
  protected ConnectionStatus getInitialStatus() {
    return ConnectionStatus.INITIAL_STATUS;
  }

  /**
   * Attempt to
   * {@link #doRefreshStatus() refresh the connection status and update the {@link #getStatus()
   * cached results}.
   *
   * <p>This method always calls {@link #doRefreshStatus()} and then on success updates
   * the {@link #getStatus() cached connection status}. If we see that the status has been updated
   * since the refresh was started, we ignore the update.
   *
   * @return the future that will complete with the updated connection status
   * @see #getConnectionStatus()
   * @see #doRefreshConnectionStatus()
   */
  public final Future<ConnectionStatus> refreshStatus() {
    var originalState = this.cachedStatus.get();
    var beforeStartingRefresh = Instant.now();

    // Always set the cached status when the future completes successfully
    return doRefreshStatus().onSuccess(updated -> {
      var lastUpdatedInstant = lastUpdated.get();
      if (lastUpdatedInstant.isAfter(beforeStartingRefresh)) {
        Log.infof(
            "Ignoring stale connection status update for %s: %s. " +
                "last updated: %s, before starting refresh time: %s",
            spec.id(),
            updated,
            lastUpdatedInstant,
            beforeStartingRefresh
        );
      } else {
        // Update the status if it has not been updated since the refresh was started
        updateStatus(originalState, updated);
      }
    });
  }

  private void updateStatus(ConnectionStatus original, ConnectionStatus updated) {
    // update the cached status
    this.cachedStatus.set(updated);
    this.lastUpdated.set(Instant.now());

    // If the status has changed, notify the listener
    if (!updated.equals(original)) {
      Log.infof(
          "Updated status for connection ID=%s. Original spec: %s, "
              + "Updated spec: %s, Last updated: %s",
          spec.id(),
          original,
          updated,
          lastUpdated.get()
      );
      if (updated.isConnected()) {
        listener.connected(this);
      } else {
        listener.disconnected(this);
      }
    }
  }

  /**
   * Refresh the connection status. By default, this simply returns a completed future with the
   * {@link #getInitialStatus() initial status}. Subclasses should override this method to implement
   * the actual connection status refresh logic.
   *
   * @return the future that will complete with the updated connection status
   * @see #refreshStatus()
   * @see #getStatus()
   */
  protected Future<ConnectionStatus> doRefreshStatus() {
    return Future.succeededFuture(getInitialStatus());
  }

  public ConnectionMetadata getConnectionMetadata() {
    return ConnectionMetadata.from(
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

  /**
   * Get the Kafka connection options for the Kafka cluster with the supplied ID.
   *
   * @return the connection options; never null
   */
  public KafkaConnectionOptions getKafkaConnectionOptions() {
    if (spec.kafkaClusterConfig() != null) {
      return CredentialsKafkaConnectionOptionsBuilder
          .builder()
          .redact(false)
          .tlsConfig(spec.kafkaClusterConfig().tlsConfig())
          .build();
    }

    return CredentialsKafkaConnectionOptionsBuilder
        .builder()
        .redact(false)
        .build();
  }

  public Credentials.SchemaRegistryConnectionOptions getSchemaRegistryOptions() {
    if (spec.schemaRegistryConfig() != null) {
      return CredentialsSchemaRegistryConnectionOptionsBuilder
          .builder()
          .redact(false)
          .tlsConfig(spec.schemaRegistryConfig().tlsConfig())
          .build();
    }

    return CredentialsSchemaRegistryConnectionOptionsBuilder
        .builder()
        .redact(false)
        .build();
  }

  /**
   * HTTP auth headers provided by the {@link ConnectionState} to use for Kafka REST API requests.
   */
  public MultiMap getKafkaAuthenticationHeaders() {
    return MultiMap.caseInsensitiveMultiMap();
  }

  /**
   * HTTP auth headers provided by the {@link ConnectionState} to use for Schema Registry API
   * requests.
   */
  public MultiMap getSchemaRegistryAuthenticationHeaders(String clusterId) {
    return MultiMap.caseInsensitiveMultiMap();
  }

  /**
   * Get the {@link Credentials} for the Kafka cluster with the supplied ID.
   *
   * @return the credentials or empty if the cluster ID is not known or the cluster requires no
   * credentials
   */
  public Optional<Credentials> getKafkaCredentials() {
    return Optional.empty();
  }

  /**
   * Get the {@link Credentials} for the Schema Registry with the supplied ID.
   *
   * @return the credentials or empty if the cluster ID is not known or the cluster requires no
   * credentials
   */
  public Optional<Credentials> getSchemaRegistryCredentials() {
    return Optional.empty();
  }

  public Optional<TLSConfig> getKafkaTLSConfig() {
    return Optional.empty();
  }

  public Optional<TLSConfig> getSchemaRegistryTLSConfig() {
    return Optional.empty();
  }
}