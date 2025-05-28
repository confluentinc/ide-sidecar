package io.confluent.idesidecar.restapi.models.graph;

import static io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType.DIRECT;

import io.confluent.idesidecar.restapi.clients.SchemaRegistryClient;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.events.ClusterKind;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.idesidecar.restapi.events.ServiceKind;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;

/**
 * A {@link DirectFetcher} that uses the {@link ConnectionStateManager} to find direct
 * {@link Connection}s and associated resources.
 *
 * <p>This fetcher makes use of
 * <a href="https://quarkus.io/guides/cdi#events-and-observers">CDI events</a>
 * so that other components can observe changes in the loaded {@link Cluster} instances. Each event
 * has the following {@link Lifecycle} qualifier:
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
  public DirectConnection getDirectConnectionByID(String connectionID) throws Exception {
    var connection = connections
        .getConnectionStates()
        .stream()
        .filter(conn -> conn.getSpec().id().equals(connectionID))
        .findFirst();

    if (connection.isPresent()) {
      var foundConnection = connection.get();
      if (!DIRECT.equals(foundConnection.getSpec().type())) {
        throw new Exception(
            "Connection with ID=" + connectionID + " is not a direct connection."
        );
      }
      return new DirectConnection(
          foundConnection.getSpec().id(),
          foundConnection.getSpec().name()
      );
    }
    return null;
  }

  @Override
  public Uni<DirectKafkaCluster> getKafkaCluster(String connectionId) {
    var state = connections.getConnectionState(connectionId);
    if (state instanceof DirectConnectionState directState) {
      if (!directState.isKafkaConnected()) {
        // Either there is no Kafka cluster configured or it is not connected, so return no info
        Log.debugf("Skipping connection '%s' since Kafka is not connected.", connectionId);
        return Uni.createFrom().nullItem();
      }
      // If there is a Kafka cluster configured, get the details
      return directState.withAdminClient(
          adminClient -> getKafkaCluster(directState, adminClient),
          error -> {
            Log.infof(
                "Unable to connect to the Kafka cluster at %s for connection '%s'",
                state.getSpec().kafkaClusterConfig().bootstrapServers(),
                connectionId,
                error
            );
            return Uni.createFrom().<DirectKafkaCluster>nullItem();
          }
      ).orElseGet(
          // There was no Kafka cluster configured, so return no info
          () -> Uni.createFrom().nullItem()
      );
    }
    // Unexpectedly not a direct connection
    Log.errorf("Connection with ID=%s is not a direct connection.", connectionId);
    return Uni.createFrom().nullItem();
  }

  protected Uni<DirectKafkaCluster> getKafkaCluster(
      DirectConnectionState state,
      AdminClient adminClient
  ) {
    var spec = state.getSpec();
    var kafkaConfig = spec.kafkaClusterConfig();
    return Uni
        .createFrom()
        .completionStage(
            // Use the client to get the cluster ID, to verify that we can connect
            adminClient.describeCluster().clusterId().toCompletionStage()
        )
        .map(clusterId ->
            new DirectKafkaCluster(
                clusterId,
                null,
                kafkaConfig.bootstrapServers(),
                state.getId()
            )
        ).map(cluster -> {
          // Emit an event that this cluster was loaded
          onLoad(state.getId(), cluster);
          // And return the cluster
          return cluster;
        });
  }

  public Uni<DirectSchemaRegistry> getSchemaRegistry(String connectionId) {
    var state = connections.getConnectionState(connectionId);
    if (state instanceof DirectConnectionState directState) {
      if (!directState.isSchemaRegistryConnected()) {
        // Either there is no Schema Registry configured or it is not connected, so return no info
        Log.debugf("Skipping connection '%s' since Schema Registry is not connected.",
            connectionId);
        return Uni.createFrom().nullItem();
      }
      // Use the SR client to obtain the cluster ID
      return directState.withSchemaRegistryClient(
          srClient -> getSchemaRegistry(directState, srClient),
          error -> {
            Log.infof(
                "Unable to connect to the Schema Registry at %s for connection '%s'",
                state.getSpec().schemaRegistryConfig().uri(),
                connectionId,
                error
            );
            return Uni.createFrom().<DirectSchemaRegistry>nullItem();
          }
      ).orElseGet(
          // There was no Schema Registry configured, so return no info
          () -> Uni.createFrom().nullItem()
      );
    }
    // Unexpectedly not a direct connection
    Log.errorf("Connection with ID=%s is not a direct connection.", connectionId);
    return Uni.createFrom().nullItem();
  }

  protected Uni<DirectSchemaRegistry> getSchemaRegistry(
      DirectConnectionState state,
      SchemaRegistryClient srClient
  ) throws RestClientException, IOException {
    // Use the client to get *some* information, to verify that we can connect
    var schemaTypes = srClient.getSchemaTypes();

    // Construct the cluster object
    var srConfig = state.getSpec().schemaRegistryConfig();
    var ccloudEndpoint = srConfig.asCCloudEndpoint();
    DirectSchemaRegistry cluster;
    if (ccloudEndpoint.isPresent()) {
      var endpoint = ccloudEndpoint.get();
      cluster = new DirectSchemaRegistry(
          endpoint.clusterId().toString(),
          endpoint.getUri().toString(),
          state.getId()
      );
    } else {
      cluster = new DirectSchemaRegistry(
          orDefault(srConfig.id(), () -> state.getId() + "-schema-registry"),
          srConfig.uri(),
          state.getId()
      );
    }
    return Uni.createFrom().item(cluster);
  }

  private String orDefault(String value, Supplier<String> defaultValue) {
    return value != null ? value : defaultValue.get();
  }
}
