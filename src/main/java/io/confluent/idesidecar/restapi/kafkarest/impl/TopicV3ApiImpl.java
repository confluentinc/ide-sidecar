package io.confluent.idesidecar.restapi.kafkarest.impl;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.kafkarest.CachedAdminClientService;
import io.confluent.idesidecar.restapi.kafkarest.api.TopicV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.UpdatePartitionCountRequestData;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.List;

// TODO: Figure out how to mount the endpoints at a different root path
@RequestScoped
public class TopicV3ApiImpl implements TopicV3Api {

  @Inject
  HttpServerRequest request;

  @Inject
  CachedAdminClientService cachedAdminClientService;

  @Override
  public Uni<TopicData> createKafkaTopic(String clusterId,
      CreateTopicRequestData createTopicRequestData) {
    return null;
  }

  @Override
  public Uni<Void> deleteKafkaTopic(String clusterId, String topicName) {
    return null;
  }

  @Override
  public Uni<TopicData> getKafkaTopic(String clusterId, String topicName,
      Boolean includeAuthorizedOperations) {
    var adminClient = cachedAdminClientService.getOrCreateAdminClient(
        request.getHeader(CONNECTION_ID_HEADER), clusterId);
    var describeTopics = adminClient.describeTopics(List.of(topicName));
    return Uni
        .createFrom()
        .completionStage(describeTopics.allTopicNames().toCompletionStage())
        // Get the first topic description
        .map(topicDescriptions -> topicDescriptions.values().iterator().next())
        .onItem().transformToUni(topicDescription -> Uni.createFrom().item(TopicData
            .builder()
            .kind("KafkaTopic")
            .topicName(topicDescription.name())
            .clusterId(clusterId)
            .partitionsCount(topicDescription.partitions().size())
            .replicationFactor(topicDescription.partitions().getFirst().replicas().size())
            .build()));
  }

  @Override
  public Uni<TopicDataList> listKafkaTopics(String clusterId) {
    return null;
  }

  @Override
  public Uni<TopicData> updatePartitionCountKafkaTopic(String clusterId, String topicName,
      UpdatePartitionCountRequestData updatePartitionCountRequestData) {
    return null;
  }
}
