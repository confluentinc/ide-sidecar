package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.PartitionData;
import io.confluent.idesidecar.restapi.kafkarest.model.PartitionDataList;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.List;

@RequestScoped
public class PartitionManagerImpl extends Manager {
  @Inject
  TopicManagerImpl topicManager;

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
                "This server does not host topic-partition %d for topic %s".formatted(
                    partitionId, topicName
                )
            ))
        );
  }

  public Uni<List<TopicPartitionInfo>> listKafkaPartitions(String clusterId, String topicName) {
    return topicManager
        .getKafkaTopic(clusterId, topicName, false)
        .onItem()
        .transform(TopicDescription::partitions);
  }
}
