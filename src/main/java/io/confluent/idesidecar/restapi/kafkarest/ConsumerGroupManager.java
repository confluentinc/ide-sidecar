package io.confluent.idesidecar.restapi.kafkarest;

import io.smallrye.mutiny.Uni;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Interface for managing Kafka consumer groups.
 * See {@link ConsumerGroupManagerImpl} for the implementation.
 */
public interface ConsumerGroupManager {

  Uni<List<ConsumerGroupDescription>> listKafkaConsumerGroups(String clusterId);

  Uni<ConsumerGroupDescription> getKafkaConsumerGroup(
      String clusterId, String consumerGroupId);

  Uni<Map<TopicPartition, OffsetAndMetadata>> listConsumerGroupOffsets(
      String clusterId, String consumerGroupId);

  Uni<Map<TopicPartition, ListOffsetsResultInfo>> getLogEndOffsets(
      String clusterId, Collection<TopicPartition> topicPartitions);
}
