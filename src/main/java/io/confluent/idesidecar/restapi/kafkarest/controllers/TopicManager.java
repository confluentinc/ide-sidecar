package io.confluent.idesidecar.restapi.kafkarest.controllers;

import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.smallrye.mutiny.Uni;

public interface TopicManager {

  Uni<TopicData> createKafkaTopic(
      String clusterId,
      CreateTopicRequestData createTopicRequestData
  );

  Uni<Void> deleteKafkaTopic(String clusterId, String topicName);

  Uni<TopicData> getKafkaTopic(
      String clusterId,
      String topicName,
      Boolean includeAuthorizedOperations
  );

  Uni<TopicDataList> listKafkaTopics(String clusterId);
}
