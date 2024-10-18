package io.confluent.idesidecar.restapi.kafkarest;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

@RequestScoped
public class PartitionManagerImpl implements PartitionManager {
  @Inject
  TopicManager topicManager;

  @Override
  public Uni<TopicPartitionInfo> getKafkaPartition(
      String clusterId, String topicName, Integer partitionId
  ) {
    return listKafkaPartitions(clusterId, topicName)
        .onItem()
        .transform(partitionInfos -> partitionInfos
            .stream()
            .filter(partitionInfo -> partitionInfo.partition() == partitionId)
            .findFirst()
            .orElseThrow(() -> new UnknownTopicOrPartitionException(
                "This server does not host this topic-partition.")
            )
        );
  }

  @Override
  public Uni<List<TopicPartitionInfo>> listKafkaPartitions(String clusterId, String topicName) {
    return topicManager
        .getKafkaTopic(clusterId, topicName, false)
        .onItem()
        .transform(TopicDescription::partitions);
  }
}
