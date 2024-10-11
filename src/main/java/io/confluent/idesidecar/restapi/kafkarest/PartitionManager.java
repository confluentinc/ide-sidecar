package io.confluent.idesidecar.restapi.kafkarest;

import io.smallrye.mutiny.Uni;
import java.util.List;
import org.apache.kafka.common.TopicPartitionInfo;


public interface PartitionManager {
  Uni<TopicPartitionInfo> getKafkaPartition(
      String clusterId, String topicName, Integer partitionId
  );

  Uni<List<TopicPartitionInfo>> listKafkaPartitions(
      String clusterId, String topicName
  );
}
