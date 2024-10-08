package io.confluent.idesidecar.restapi.kafkarest.impl;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.kafkarest.api.TopicV3Api;
import io.confluent.idesidecar.restapi.kafkarest.controllers.AdminClientService;
import io.confluent.idesidecar.restapi.kafkarest.controllers.ClusterManagerImpl;
import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.UpdatePartitionCountRequestData;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PathParam;

@RequestScoped
public class TopicV3ApiImpl implements TopicV3Api {

  @PathParam("cluster_id")
  private String clusterId;

  @HeaderParam(CONNECTION_ID_HEADER)
  private String connectionId;

  @Inject
  AdminClientService adminClientService;

  @Override
  public Uni<TopicData> createKafkaTopic(String clusterId,
      CreateTopicRequestData createTopicRequestData) {
      return new ClusterManagerImpl(
          adminClientService.getAdminClientConfig(connectionId)
      ).createKafkaTopic(clusterId, createTopicRequestData);
  }

  @Override
  public Uni<Void> deleteKafkaTopic(String clusterId, String topicName) {
      return new ClusterManagerImpl(
          adminClientService.getAdminClientConfig(connectionId)
      ).deleteKafkaTopic(clusterId, topicName);
  }

  @Override
  public Uni<TopicData> getKafkaTopic(
      String clusterId, String topicName, Boolean includeAuthorizedOperations
  ) {
      return new ClusterManagerImpl(
          adminClientService.getAdminClientConfig(connectionId)
      ).getKafkaTopic(clusterId, topicName, includeAuthorizedOperations);

  }

  @Override
  public Uni<TopicDataList> listKafkaTopics(String clusterId) {
      return new ClusterManagerImpl(
          adminClientService.getAdminClientConfig(connectionId)
      ).listKafkaTopics(clusterId);

  }

  @Override
  public Uni<TopicData> updatePartitionCountKafkaTopic(String clusterId, String topicName,
      UpdatePartitionCountRequestData updatePartitionCountRequestData) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
