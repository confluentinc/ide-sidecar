package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.smallrye.mutiny.Uni;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.List;

/**
 * Interface for managing Kafka topics. See {@link TopicManagerImpl} for the implementation.
 */
public interface TopicManager {

  Uni<TopicDescription> createKafkaTopic(String clusterId,
      CreateTopicRequestData createTopicRequestData);

  Uni<Void> deleteKafkaTopic(String clusterId, String topicName);

  Uni<TopicDescription> getKafkaTopic(
      String clusterId, String topicName, Boolean includeAuthorizedOperations
  );

  Uni<List<TopicDescription>> listKafkaTopics(String clusterId, Boolean includeAuthorizedOperations);
}
