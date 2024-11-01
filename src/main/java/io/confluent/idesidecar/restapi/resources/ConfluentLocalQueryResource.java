package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.models.graph.ConfluentLocalKafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.LocalConnection;
import io.confluent.idesidecar.restapi.models.graph.LocalSchemaRegistry;
import io.confluent.idesidecar.restapi.models.graph.RealLocalFetcher;
import io.quarkus.logging.Log;
import io.smallrye.graphql.api.Nullable;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.List;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.NonNull;
import org.eclipse.microprofile.graphql.Query;
import org.eclipse.microprofile.graphql.Source;

/**
 * GraphQL API for querying Confluent Local resources.
 */
@GraphQLApi
public class ConfluentLocalQueryResource {

  @Inject
  RealLocalFetcher local;

  /**
   * Get all {@link LocalConnection}s.
   *
   * @return the local connections; never null but possibly empty
   */
  @Query("localConnections")
  @Description("Get all local connections")
  @NonNull
  public Uni<List<LocalConnection>> getLocalConnections() {
    return Uni.createFrom().item(local.getConnections());
  }

  /**
   * Get the local Kafka clusters to the given local connection.
   *
   * @param connection the local connection
   * @return the Kafka cluster; null if no Confluent Local Kafka cluster found
   */
  @Nullable
  public Uni<ConfluentLocalKafkaCluster> getKafkaCluster(@Source LocalConnection connection) {
    Log.infof("Get local kafka clusters for connection %s", connection.getId());
    String connectionId = connection.getId();
    return local.getKafkaCluster(connectionId);
  }

  /**
   * Get the local schema registry for the specified {@link LocalConnection}.
   *
   * @param connection the local connection
   * @return the schema registry information; may be null
   */
  @Nullable
  public Uni<LocalSchemaRegistry> getSchemaRegistry(@Source LocalConnection connection) {
    Log.infof("Get local schema registry for connection %s", connection.getId());
    String connectionId = connection.getId();
    return local.getSchemaRegistry(connectionId);
  }
}