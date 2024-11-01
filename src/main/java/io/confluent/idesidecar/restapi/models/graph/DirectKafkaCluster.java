package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;
import io.smallrye.graphql.api.Nullable;

/**
 * A Kafka cluster running remotely or locally.
 *
 * @param id                the ID of the cluster
 * @param uri               the URI of the REST API
 * @param bootstrapServers  the broker's bootstrap servers
 * @param connectionId      the ID of the connection that accessed this cluster
 */
@RegisterForReflection
@DefaultNonNull
public record DirectKafkaCluster(
    String id,
    @Nullable String uri,
    String bootstrapServers,
    String connectionId
) implements KafkaCluster {

  public DirectKafkaCluster withConnectionId(String connectionId) {
    return new DirectKafkaCluster(
        id,
        uri,
        bootstrapServers,
        connectionId
    );
  }
}
