package io.confluent.idesidecar.restapi.models.graph;

import static io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType.DIRECT;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.events.ClusterKind;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.idesidecar.restapi.events.ServiceKind;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.Connection;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import java.util.List;
import java.util.function.Supplier;

/**
 * A {@link DirectFetcher} that uses the {@link ConnectionStateManager} to find direct
 * {@link Connection}s and associated resources.
 *
 * <p>This fetcher makes use of
 * <a href="https://quarkus.io/guides/cdi#events-and-observers">CDI events</a>
 * so that other components can observe changes in the loaded {@link Cluster} instances.
 * Each event has the following {@link Lifecycle} qualifier:
 * <ul>
 *   <li>{@link Lifecycle.Updated}</li>
 * </ul>
 * and the following {@link ServiceKind} qualifiers:
 * <ul>
 *   <li>{@link ServiceKind.Local}</li>
 * </ul>
 * and one of the following {@link ClusterKind} qualifiers (depending on the kind of cluster):
 * <ul>
 *   <li>{@link ClusterKind.Kafka}</li>
 *   <li>{@link ClusterKind.SchemaRegistry}</li>
 * </ul>
 *
 * @see ClusterEvent
 */
@ApplicationScoped
@RegisterForReflection
public class RealDirectFetcher extends ConfluentRestClient implements DirectFetcher {

  @Inject
  Event<ClusterEvent> clusterEvents;

  // TODO: DIRECT fetcher should use logic similar to RealLocalFetcher to find the cluster
  // information from a Kafka REST URL endpoint, if it is available.
  // That is left to future improvements.

  /**
   * Construct the headers that will be used for REST requests made by this fetcher.
   * This {@link RealDirectFetcher} will only submit REST requests to the Kafka REST proxy
   * of a direct connection, in order to discover the Kafka cluster details.
   * It will never submit REST requests to a direct connection's Schema Registry.
   *
   * <p>Therefore, this method only constructs the headers using the direct connection's
   * Kafka credentials.
   *
   * @param connectionId the connection ID
   * @return the headers
   * @throws ConnectionNotFoundException if the connection does not exist or is not a
   *                                     direct connection
   */
  @Override
  protected MultiMap headersFor(String connectionId) throws ConnectionNotFoundException {
    var connectionState = connections.getConnectionState(connectionId);
    // Direct connections might only use the Kafka REST proxy of a direct connection
    // (and never the SR REST API). So not use REST clients, so don't include the headers in the request
    if (connectionState instanceof DirectConnectionState directConnectionState) {
      return directConnectionState.getAuthenticationHeaders(ClusterType.KAFKA);
    }
    throw new ConnectionNotFoundException(
        String.format("Connection with ID=%s is not a direct connection.", connectionId)
    );
  }

  <ClusterT extends Cluster> ClusterT onLoad(String connectionId, ClusterT cluster) {
    // Fire an event for this cluster
    ClusterEvent.onLoad(
        clusterEvents,
        cluster,
        DIRECT,
        connectionId
    );
    return cluster;
  }

  public List<DirectConnection> getConnections() {
    return connections
        .getConnectionStates()
        .stream()
        .filter(connection -> DIRECT.equals(connection.getSpec().type()))
        .map(connection -> new DirectConnection(
            connection.getSpec().id(),
            connection.getSpec().name()
        ))
        .toList();
  }

  @Override
  public Uni<DirectKafkaCluster> getKafkaCluster(String connectionId) {
    var state = connections.getConnectionState(connectionId);
    if (state instanceof DirectConnectionState directState) {
      var spec = state.getSpec();
      // Use the admin API to obtain the cluster ID
      return directState.withAdminClient(adminClient -> {
        var kafkaConfig = spec.kafkaClusterConfig();
        var describeResult = adminClient.describeCluster();
        return Uni
            .createFrom()
            .completionStage(describeResult.clusterId().toCompletionStage())
            .map(clusterId ->
              new DirectKafkaCluster(
                  clusterId,
                  null,
                  kafkaConfig.bootstrapServers(),
                  connectionId
              )
            ).map(cluster -> {
              // Emit an event that this cluster was loaded
              onLoad(connectionId, cluster);
              // And return the cluster
              return cluster;
            });
      }).orElseGet(() -> Uni.createFrom().nullItem());
    }
    // Unexpectedly not a direct connection
    Log.errorf("Connection with ID=%s is not a direct connection.", connectionId);
    return Uni.createFrom().nullItem();
  }

  public Uni<DirectSchemaRegistry> getSchemaRegistry(String connectionId) {
    var state = connections.getConnectionState(connectionId);
    if (state instanceof DirectConnectionState directState) {
      var spec = state.getSpec();
      // Use the admin API to obtain the cluster ID
      return directState.withSchemaRegistryClient(srClient -> {
        Uni<DirectSchemaRegistry> result;
        try {
          var mode = srClient.getMode(); // unused, but we know we can connect
          var srConfig = spec.schemaRegistryConfig();
          var ccloudEndpoint = srConfig.asCCloudEndpoint();
          DirectSchemaRegistry cluster;
          if (ccloudEndpoint.isPresent()) {
            var endpoint = ccloudEndpoint.get();
            cluster = new DirectSchemaRegistry(
                endpoint.clusterId().toString(),
                endpoint.getUri().toString(),
                connectionId
            );
          } else {
            cluster = new DirectSchemaRegistry(
                orDefault(srConfig.id(), () -> connectionId + "-schema-registry"),
                srConfig.uri(),
                connectionId
            );
          }
          result = Uni.createFrom().item(cluster);
        } catch (Throwable t) {
          Log.debugf("Failed to create schema registry client for connection ID=%s", connectionId, t);
          result = Uni.createFrom().nullItem();
        }
        return result;
      }).orElseGet(() -> Uni.createFrom().nullItem());
    }
    // Unexpectedly not a direct connection
    Log.errorf("Connection with ID=%s is not a direct connection.", connectionId);
    return Uni.createFrom().nullItem();
  }

  private String orDefault(String value, Supplier<String> defaultValue) {
    return value != null ? value : defaultValue.get();
  }
}
