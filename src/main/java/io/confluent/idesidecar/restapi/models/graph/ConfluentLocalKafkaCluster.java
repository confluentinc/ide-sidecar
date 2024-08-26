package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;
import io.smallrye.graphql.api.Nullable;

/**
 * A local Kafka cluster running in a Confluent Local Docker image.
 *
 * @param id                the ID of the cluster
 * @param name              the name of the cluster
 * @param uri               the URI of the REST API
 * @param kafkaRestHostName the hostname of the Kafka REST Proxy
 * @param bootstrapServers  the broker's bootstrap servers
 * @param connectionId      the ID of the connection that accessed this cluster
 */
@RegisterForReflection
@DefaultNonNull
public record ConfluentLocalKafkaCluster(
    String id,
    String name,
    String uri,
    @Nullable String kafkaRestHostName,
    String bootstrapServers,
    String connectionId
) implements KafkaCluster {

  public ConfluentLocalKafkaCluster(
      String id,
      String name,
      String uri,
      String kafkaRestHostName,
      String bootstrapServers
  ) {
    this(id, name, uri, kafkaRestHostName, bootstrapServers, null);
  }

  public ConfluentLocalKafkaCluster withConnectionId(String connectionId) {
    return new ConfluentLocalKafkaCluster(
        id,
        name,
        uri,
        kafkaRestHostName,
        bootstrapServers,
        connectionId
    );
  }
}
