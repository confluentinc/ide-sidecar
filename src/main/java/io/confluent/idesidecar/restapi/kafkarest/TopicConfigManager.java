package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.AlterConfigBatchRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.UpdateConfigRequestData;
import io.smallrye.mutiny.Uni;
import org.apache.kafka.clients.admin.ConfigEntry;

import java.util.List;

/**
 * Interface for querying Kafka clusters. See {@link ClusterManagerImpl} for the implementation.
 */
public interface TopicConfigManager {

  Uni<List<ConfigEntry>> listKafkaTopicConfigs(String clusterId, String topicName);

  Uni<Void> updateKafkaTopicConfig(String clusterId, String topicName, String name, UpdateConfigRequestData updateConfigRequestData);

  Uni<Void> updateKafkaTopicConfigBatch(String clusterId, String topicName, AlterConfigBatchRequestData alterConfigBatchRequestData);
}
