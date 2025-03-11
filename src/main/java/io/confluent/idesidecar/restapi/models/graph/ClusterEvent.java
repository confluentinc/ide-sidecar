package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.events.Events;
import io.confluent.idesidecar.restapi.events.Events.ConnectionTypeQualifier;
import io.confluent.idesidecar.restapi.events.Events.LifecycleQualifier;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.util.AnnotationLiteral;

/**
 * {@link ClusterEvent} events are sent on an {@link Event} channel to listeners, with information
 * about the {@link Cluster} representations loaded by the GraphQL model.
 *
 * @param connectionId   the ID of the connection used to access the cluster
 * @param connectionType the type of cluster
 * @param cluster        the cluster information
 */
public record ClusterEvent(
    String connectionId,
    ConnectionType connectionType,
    Cluster cluster
) {

  public static <ClusterT extends Cluster> ClusterT onLoad(
      Event<ClusterEvent> channel,
      ClusterT cluster,
      ConnectionType connectionType,
      String connectionId
  ) {
    if (cluster != null) {
      // Fire a event
      Events.fireAsyncEvent(
          channel,
          new ClusterEvent(connectionId, connectionType, cluster),
          LifecycleQualifier.updated(),
          ConnectionTypeQualifier.typeQualifier(connectionType),
          typeQualifierFor(cluster)
      );
    }
    return cluster;
  }

  static <ClusterT extends Cluster> AnnotationLiteral<?> typeQualifierFor(
      ClusterT cluster
  ) {
    if (cluster instanceof KafkaCluster) {
      return Events.ClusterTypeQualifier.kafkaCluster();
    } else if (cluster instanceof SchemaRegistry) {
      return Events.ClusterTypeQualifier.schemaRegistry();
    }
    return null;
  }
}
