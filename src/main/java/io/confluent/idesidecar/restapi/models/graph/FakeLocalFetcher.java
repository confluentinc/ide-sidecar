package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

/**
 * A {@link LocalFetcher} that always returns fixed, consistent and fake resource objects.
 */
@ApplicationScoped
@RegisterForReflection
public class FakeLocalFetcher implements LocalFetcher {

  public static final String CONNECTION_ID = "e67c02d0-71b3-41b9-8747-6aee2abd6032";

  public List<LocalConnection> getConnections() {
    return List.of(
        new LocalConnection(CONNECTION_ID)
    );
  }

  @Override
  public Uni<ConfluentLocalKafkaCluster> getKafkaCluster(String connectionId) {
    return Uni.createFrom().item(new ConfluentLocalKafkaCluster(
        "confluent-local",
        "Local Kafka Cluster",
        "http://localhost:8082",
        "localhost",
        "localhost:9092",
        connectionId
    ));
  }

  @Override
  public Uni<LocalSchemaRegistry> getSchemaRegistry(String connectionId) {
    return Uni.createFrom().item(new LocalSchemaRegistry("http://localhost:8081", connectionId));
  }
}
