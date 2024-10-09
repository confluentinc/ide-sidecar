package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.api.TopicV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.UpdatePartitionCountRequestData;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;

@RequestScoped
public class TopicV3ApiImpl implements TopicV3Api {

  @Inject
  TopicManagerImpl topicManager;

  @Override
  public Uni<TopicData> createKafkaTopic(
      String clusterId, CreateTopicRequestData createTopicRequestData
  ) {
    return topicManager.createKafkaTopic(clusterId, createTopicRequestData);
  }

  @Override
  public Uni<Void> deleteKafkaTopic(String clusterId, String topicName) {
    return topicManager.deleteKafkaTopic(clusterId, topicName);
  }

  @Override
  public Uni<TopicData> getKafkaTopic(
      String clusterId, String topicName, Boolean includeAuthorizedOperations
  ) {
    return topicManager.getKafkaTopic(clusterId, topicName, includeAuthorizedOperations);
  }

  @Override
  public Uni<TopicDataList> listKafkaTopics(String clusterId) {
    return topicManager.listKafkaTopics(clusterId);
  }

  @Override
  public Uni<TopicData> updatePartitionCountKafkaTopic(
      String clusterId,
      String topicName,
      UpdatePartitionCountRequestData updatePartitionCountRequestData
  ) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
