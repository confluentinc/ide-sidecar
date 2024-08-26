package io.confluent.idesidecar.restapi.models.graph;

import io.smallrye.mutiny.Uni;
import java.util.List;

public interface LocalFetcher {

  /**
   * Get all local connections.
   *
   * @return the list of all local connections; never null but possibly empty
   */
  List<LocalConnection> getConnections();

  /**
   * Get the local Kafka cluster or broker.
   *
   * @param connectionId the ID of the connection
   * @return the local Kafka cluster; may be null
   */
  Uni<ConfluentLocalKafkaCluster> getKafkaCluster(String connectionId);

  /**
   * Get the local Schema Registry instance.
   *
   * @param connectionId the ID of the connection
   * @return the local Kafka cluster; may be null
   */
  Uni<LocalSchemaRegistry> getSchemaRegistry(String connectionId);
}
