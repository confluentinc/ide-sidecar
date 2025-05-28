package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.models.graph.DirectConnection;
import io.confluent.idesidecar.restapi.models.graph.DirectKafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.DirectSchemaRegistry;
import io.confluent.idesidecar.restapi.models.graph.LocalConnection;
import io.confluent.idesidecar.restapi.models.graph.RealDirectFetcher;
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
 * GraphQL API for querying direct connection resources.
 */
@GraphQLApi
public class DirectQueryResource {

  @Inject
  RealDirectFetcher direct;

  /**
   * Get all {@link DirectConnection}s.
   *
   * @return the connections; never null but possibly empty
   */
  @Query("directConnections")
  @Description("Get all direct connections")
  @NonNull
  public Uni<List<DirectConnection>> getDirectConnections() {
    return Uni.createFrom().item(direct.getConnections());
  }

  /**
   * Get {@link DirectConnection} by ID.
   *
   * @return the connection
   */
  @Query("directConnectionById")
  @Description("Get direct connection by ID")
  @NonNull
  public DirectConnection getDirectConnectionByID(String connectionID) throws Exception {
    return direct.getDirectConnectionByID(connectionID);
  }

  /**
   * Get the Kafka cluster for the given direct connection.
   *
   * @param connection the direct connection
   * @return the Kafka cluster; null if no Confluent Local Kafka cluster found
   */
  @Nullable
  public Uni<DirectKafkaCluster> getKafkaCluster(@Source DirectConnection connection) {
    Log.infof("Get Kafka cluster for direct connection %s", connection.getId());
    String connectionId = connection.getId();
    return direct.getKafkaCluster(connectionId);
  }

  /**
   * Get the schema registry for the specified {@link LocalConnection}.
   *
   * @param connection the direct connection
   * @return the schema registry information; may be null
   */
  @Nullable
  public Uni<DirectSchemaRegistry> getSchemaRegistry(@Source DirectConnection connection) {
    Log.infof("Get Schema Registry for direct connection %s", connection.getId());
    String connectionId = connection.getId();
    return direct.getSchemaRegistry(connectionId);
  }
}