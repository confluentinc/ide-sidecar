package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.clients.AdminClients;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

/**
 * RequestScoped bean for managing Kafka consumer groups. Creating the bean as
 * {@link RequestScoped} allows us to inject the {@link HttpServerRequest} which is
 * used to get the connection ID from the request headers.
 */
@RequestScoped
public class ConsumerGroupManagerImpl implements ConsumerGroupManager {

  @Inject
  AdminClients adminClients;

  @Inject
  HttpServerRequest request;

  Supplier<String> connectionId = () -> request.getHeader(CONNECTION_ID_HEADER);

  @Inject
  ClusterManager clusterManager;

  @Override
  public Uni<List<ConsumerGroupDescription>> listKafkaConsumerGroups(String clusterId) {
    return clusterManager.getKafkaCluster(clusterId)
        .chain(ignored -> uniStage(
            adminClients.getClient(connectionId.get(), clusterId)
                .listConsumerGroups()
                .all()
                .toCompletionStage()
        ))
        .chain(listings -> {
          var groupIds = listings.stream()
              .map(listing -> listing.groupId())
              .toList();
          if (groupIds.isEmpty()) {
            return Uni.createFrom().item(List.of());
          }
          return uniStage(
              adminClients.getClient(connectionId.get(), clusterId)
                  .describeConsumerGroups(groupIds)
                  .all()
                  .toCompletionStage()
          ).map(descriptions -> descriptions.values().stream().toList());
        });
  }

  @Override
  public Uni<ConsumerGroupDescription> getKafkaConsumerGroup(
      String clusterId, String consumerGroupId
  ) {
    return clusterManager.getKafkaCluster(clusterId)
        .chain(ignored -> uniStage(
            adminClients.getClient(connectionId.get(), clusterId)
                .describeConsumerGroups(List.of(consumerGroupId))
                .all()
                .toCompletionStage()
        ))
        .map(descriptions -> {
          var desc = descriptions.get(consumerGroupId);
          // Kafka returns a DEAD description for non-existent groups
          // rather than throwing an exception
          if (desc.state() == ConsumerGroupState.DEAD) {
            throw new GroupIdNotFoundException(
                "Consumer group '%s' not found.".formatted(consumerGroupId));
          }
          return desc;
        });
  }

  @Override
  public Uni<Map<TopicPartition, OffsetAndMetadata>> listConsumerGroupOffsets(
      String clusterId, String consumerGroupId
  ) {
    return clusterManager.getKafkaCluster(clusterId)
        .chain(ignored -> uniStage(
            adminClients.getClient(connectionId.get(), clusterId)
                .listConsumerGroupOffsets(consumerGroupId)
                .partitionsToOffsetAndMetadata()
                .toCompletionStage()
        ));
  }

  @Override
  public Uni<Map<TopicPartition, ListOffsetsResultInfo>> getLogEndOffsets(
      String clusterId, Collection<TopicPartition> topicPartitions
  ) {
    if (topicPartitions.isEmpty()) {
      return Uni.createFrom().item(Map.of());
    }
    var offsetSpecs = topicPartitions.stream()
        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
    return clusterManager.getKafkaCluster(clusterId)
        .chain(ignored -> uniStage(
            adminClients.getClient(connectionId.get(), clusterId)
                .listOffsets(offsetSpecs)
                .all()
                .toCompletionStage()
        ));
  }
}
