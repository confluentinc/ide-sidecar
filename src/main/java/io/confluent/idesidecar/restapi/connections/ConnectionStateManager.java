package io.confluent.idesidecar.restapi.connections;

import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext;
import io.confluent.idesidecar.restapi.connections.ConnectionState.StateChangedListener;
import io.confluent.idesidecar.restapi.events.Events;
import io.confluent.idesidecar.restapi.events.Events.ConnectionTypeQualifier;
import io.confluent.idesidecar.restapi.events.Events.LifecycleQualifier;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.idesidecar.restapi.events.ServiceKind;
import io.confluent.idesidecar.restapi.exceptions.CCloudAuthenticationFailedException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.CreateConnectionException;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.confluent.idesidecar.restapi.exceptions.InvalidInputException;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The {@link ConnectionStateManager} allows to manage a set of {@link ConnectionState}s. It
 * provides CRUD operations for creating, reading, updating and deleting them.
 *
 * <p>This manager makes use of
 * <a href="https://quarkus.io/guides/cdi#events-and-observers">CDI events</a>
 * so that other components can observe changes in the managed {@link ConnectionState} instances.
 * Each event has one of the following {@link Lifecycle} qualifiers:
 * <ul>
 *   <li>{@link Lifecycle.Created}</li>
 *   <li>{@link Lifecycle.Updated}</li>
 *   <li>{@link Lifecycle.Connected}</li>
 *   <li>{@link Lifecycle.Disconnected}</li>
 *   <li>{@link Lifecycle.Deleted}</li>
 * </ul>
 * and one of the following {@link ServiceKind} qualifiers:
 * <ul>
 *   <li>{@link ServiceKind.CCloud}</li>
 *   <li>{@link ServiceKind.ConfluentPlatform}</li>
 *   <li>{@link ServiceKind.Local}</li>
 * </ul>
 */
@ApplicationScoped
public class ConnectionStateManager {

  @Inject
  UuidFactory idFactory;

  /**
   * A channel used to fire events whenever a managed ConnectionState changes.
   */
  @Inject
  Event<ConnectionState> connectionStateEvents;

  private final Map<String, ConnectionState> connectionStates = new HashMap<>();

  private static final String CONNECTION_NOT_FOUND = "Connection %s is not found.";

  /**
   * The listener that captures when individual ConnectionState instances connect and disconnect.
   * The methods fire events via the {@link #connectionStateEvents}
   */
  private final StateChangedListener stateChangeListener = new StateChangedListener() {

    @Override
    public void connected(ConnectionState connection) {
      if (isConnection(connection)) {
        Events.fireAsyncEvent(
            connectionStateEvents,
            connection,
            LifecycleQualifier.connected(),
            ConnectionTypeQualifier.typeQualifier(connection)
        );
      }
    }

    @Override
    public void disconnected(ConnectionState connection) {
      if (isConnection(connection)) {
        Events.fireAsyncEvent(
            connectionStateEvents,
            connection,
            LifecycleQualifier.disconnected(),
            ConnectionTypeQualifier.typeQualifier(connection)
        );
      }
    }
  };

  boolean isConnection(ConnectionState connection) {
    return connection != null
           && connection.getSpec() != null
           && connectionStates.containsKey(connection.getSpec().id());
  }

  public ConnectionState getConnectionState(String id) throws ConnectionNotFoundException {
    if (connectionStates.containsKey(id)) {
      return connectionStates.get(id);
    }
    throw new ConnectionNotFoundException(String.format(CONNECTION_NOT_FOUND, id));
  }

  /**
   * Retrieve the {@link ConnectionState} having a specific {@link ConnectionState#getInternalId()}.
   *
   * @param internalId of the {@link ConnectionState}, cannot be null.
   * @return the {@link ConnectionState} having the provided internalId.
   * @throws NullPointerException if the provided internalId is null.
   * @throws ConnectionNotFoundException if no {@link ConnectionState} having the provided
   *         internalId exists.
   */
  public ConnectionState getConnectionStateByInternalId(String internalId)
      throws ConnectionNotFoundException {

    Objects.requireNonNull(internalId);

    return connectionStates
        .values().stream()
        .filter(connectionState -> internalId.equals(connectionState.getInternalId()))
        .findFirst()
        .orElseThrow(() -> new ConnectionNotFoundException(
            String.format("Connection with internalId=%s is not found.", internalId)));
  }

  public ConnectionSpec getConnectionSpec(String id) throws ConnectionNotFoundException {
    if (connectionStates.containsKey(id)) {
      return connectionStates.get(id).getSpec();
    }
    throw new ConnectionNotFoundException(String.format(CONNECTION_NOT_FOUND, id));
  }

  public List<ConnectionState> getConnectionStates() {
    return connectionStates.values().stream().toList();
  }

  /**
   * Test whether the given connection spec is valid and can be used to create a connection state.
   * This does not store the resulting connection state, and does not
   * {@link ConnectionState#refreshStatus()} of the resulting connection state.
   *
   * @param spec the specification for the connection
   * @return the connection state that would have been created
   * @throws CreateConnectionException if the spec includes an ID that already exists
   * @throws InvalidInputException if the spec is not valid
   */
  public ConnectionState testConnectionState(
      ConnectionSpec spec
  ) throws CreateConnectionException {
    return createConnectionState(spec, true);
  }

  /**
   * Validate the given connection spec and when valid create a new connection with that spec.
   *
   * @param spec the specification for the connection
   * @return the new connection state
   * @throws CreateConnectionException if the spec includes an ID that already exists
   * @throws InvalidInputException if the spec is not valid
   */
  public ConnectionState createConnectionState(
      ConnectionSpec spec
  ) throws CreateConnectionException {
    return createConnectionState(spec, false);
  }

  private synchronized ConnectionState createConnectionState(
      ConnectionSpec spec,
      boolean dryRun
  ) throws CreateConnectionException {
    final ConnectionSpec originalSpec = spec;
    if (spec.id() == null) {
      // Generate a new ID for this new spec
      spec = spec.withId(idFactory.getRandomUuid());
    }
    if (connectionStates.containsKey(spec.id())) {
      throw new CreateConnectionException(
          String.format("There is already a connection with ID '%s'.", spec.id())
      );
    }
    // Validate the spec
    var errors = spec.validate();
    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }

    if (dryRun) {
      // Use the original spec (without the generated ID), and do not specify a listener
      return ConnectionStates.from(originalSpec, null);
    }

    // Otherwise create the connection state and store it
    var connection = ConnectionStates.from(spec, stateChangeListener);
    connectionStates.put(spec.id(), connection);

    // And fire a event signaling the creation
    Events.fireAsyncEvent(
        connectionStateEvents,
        connection,
        LifecycleQualifier.created(),
        ConnectionTypeQualifier.typeQualifier(connection)
    );

    // If the connection is initially connected, fire a connected event
    if (connection.getStatus().isConnected()) {
      stateChangeListener.connected(connection);
    }

    return connection;
  }

  public Uni<Void> updateSpecForConnectionState(String id, ConnectionSpec newSpec) {
    // Check if the connection state exists
    if (!connectionStates.containsKey(id)) {
      return Uni.createFrom().failure(
          new ConnectionNotFoundException(CONNECTION_NOT_FOUND.formatted(id)));
    }

    return Uni.createFrom()
        .item(() -> getConnectionSpec(id).validateUpdate(newSpec))
        .onItem()
        .transformToUni(errors -> {
          if (!errors.isEmpty()) {
            return Uni.createFrom().failure(new InvalidInputException(errors));
          }
          return Uni.createFrom().voidItem();
        })
        .chain(ignored -> switch (newSpec.type()) {
          // Make sure the Confluent Cloud organization ID is valid for this user
          case CCLOUD -> validateCCloudOrganizationId(id, newSpec.ccloudOrganizationId());
          // TODO: DIRECT connection changes need to be validated
          case DIRECT -> Uni.createFrom().voidItem();
          // No need to validate the spec for LOCAL, DIRECT, and PLATFORM connections
          case LOCAL, PLATFORM -> Uni.createFrom().voidItem();
        })
        .chain(ignored -> {
          // Get and update the connection spec after all validations are green
          var updated = connectionStates.get(id);
          updated.setSpec(newSpec);

          // Fire an event
          Events.fireAsyncEvent(
              connectionStateEvents,
              updated,
              LifecycleQualifier.updated(),
              ConnectionTypeQualifier.typeQualifier(updated)
          );

          return Uni.createFrom().voidItem();
        });
  }

  public Uni<Void> patchSpecForConnectionState(String id, ConnectionSpec newSpec) {
    // Check if the connection state exists
    if (!connectionStates.containsKey(id)) {
      return Uni.createFrom().failure(
          new ConnectionNotFoundException(CONNECTION_NOT_FOUND.formatted(id)));
    }

    // Get the existing spec
    ConnectionSpec existingSpec = getConnectionSpec(id);

    // Merge the new spec into the existing spec
    ConnectionSpec mergedSpec = existingSpec.merge(newSpec);

    return Uni.createFrom()
        .item(() -> mergedSpec.validateUpdate(newSpec))
        .onItem()
        .transformToUni(errors -> {
          if (!errors.isEmpty()) {
            return Uni.createFrom().failure(new InvalidInputException(errors));
          }
          return Uni.createFrom().voidItem();
        })
        .chain(ignored -> switch (mergedSpec.type()) {
          // Make sure the Confluent Cloud organization ID is valid for this user
          case CCLOUD -> validateCCloudOrganizationId(id, mergedSpec.ccloudOrganizationId());
          // TODO: DIRECT connection changes need to be validated
          case DIRECT -> Uni.createFrom().voidItem();
          // No need to validate the spec for LOCAL, DIRECT, and PLATFORM connections
          case LOCAL, PLATFORM -> Uni.createFrom().voidItem();
        })
        .chain(ignored -> {
          // Get and update the connection spec after all validations are green
          var updated = connectionStates.get(id);
          updated.setSpec(mergedSpec);

          // Fire an event
          Events.fireAsyncEvent(
              connectionStateEvents,
              updated,
              LifecycleQualifier.updated(),
              ConnectionTypeQualifier.typeQualifier(updated)
          );

          return Uni.createFrom().voidItem();
        });
  }

  /**
   * Validate the provided Confluent Cloud configuration for a connection. We do this by
   * refreshing the OAuth tokens using the provided organization ID. If the organization ID is
   * invalid, we return a 400 error.
   * @param id the ID of the connection
   * @param organizationId the Confluent Cloud organization ID
   */
  private Uni<Void> validateCCloudOrganizationId(String id, String organizationId) {
    var connectionState = (CCloudConnectionState) connectionStates.get(id);
    CCloudOAuthContext authContext = connectionState.getOauthContext();

    return Uni.createFrom()
        .completionStage(authContext
            .refreshIgnoreFailures(organizationId).toCompletionStage())
        .onFailure(CCloudAuthenticationFailedException.class).recoverWithUni(
            error -> {
              // If Confluent Cloud tells us that the organization ID is invalid, return a 400
              if (error.getMessage().contains("invalid resource id")) {
                return Uni.createFrom().failure(new InvalidInputException(List.of(Error.create()
                  .withSource("ccloud_config.organization_id")
                  .withTitle("Invalid organization ID")
                  .withDetail("Could not authenticate with the provided organization ID: %s"
                      .formatted(organizationId))
                )));
              } else {
                return Uni.createFrom().failure(error);
              }
            }
        )
        .replaceWith(Uni.createFrom().voidItem());
  }

  public void deleteConnectionState(String id) throws ConnectionNotFoundException {
    // Try to delete the connection with this id
    var removed = connectionStates.remove(id);
    if (removed == null) {
      // There was no connection with this id
      throw new ConnectionNotFoundException(String.format(CONNECTION_NOT_FOUND, id));
    }
    // Removal was successful, so fire a event
    Events.fireAsyncEvent(
        connectionStateEvents,
        removed,
        LifecycleQualifier.deleted(),
        ConnectionTypeQualifier.typeQualifier(removed)
    );
  }

  // This method should be only used in tests.
  public void clearAllConnectionStates() {
    connectionStates.clear();
  }
}
