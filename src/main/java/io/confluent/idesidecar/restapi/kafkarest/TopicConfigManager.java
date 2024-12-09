package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.AlterConfigBatchRequestData;
import io.smallrye.mutiny.Uni;
import java.util.List;
import org.apache.kafka.clients.admin.ConfigEntry;

/**
 * Interface for querying Kafka clusters. See {@link ClusterManagerImpl} for the implementation.
 */
public interface TopicConfigManager {

  Uni<List<ConfigEntry>> listKafkaTopicConfigs(String clusterId, String topicName);

  Uni<Void> updateKafkaTopicConfigBatch(String clusterId, String topicName,
      AlterConfigBatchRequestData alterConfigBatchRequestData);
}
