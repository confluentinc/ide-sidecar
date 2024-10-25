package io.confluent.idesidecar.restapi.models.graph;

import io.smallrye.mutiny.Uni;
import java.util.List;

public interface DirectFetcher {

  /**
   * Get all direct connections.
   *
   * @return the list of all direct connections; never null but possibly empty
   */
  List<DirectConnection> getConnections();

  /**
   * Get the Kafka cluster or broker.
   *
   * @param connectionId the ID of the connection
   * @return the local Kafka cluster; may be null
   */
  Uni<DirectKafkaCluster> getKafkaCluster(String connectionId);

  /**
   * Get the Schema Registry instance.
   *
   * @param connectionId the ID of the connection
   * @return the local Kafka cluster; may be null
   */
  Uni<DirectSchemaRegistry> getSchemaRegistry(String connectionId);
}
