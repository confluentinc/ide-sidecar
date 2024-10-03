package io.confluent.idesidecar.restapi.kafkarest.impl;

import io.confluent.idesidecar.restapi.kafkarest.api.TopicV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.Error;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.UpdatePartitionCountRequestData;
import io.confluent.idesidecar.restapi.util.MutinyUtil;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

@RequestScoped
public class TopicV3ApiImpl implements TopicV3Api {

  // TODO: Check passed cluster_id matches adminClient's bootstrap servers

  @Inject
  AdminClient adminClient;

  @PathParam("cluster_id")
  private String clusterId;

  @Override
  public Uni<TopicData> createKafkaTopic(String clusterId,
      CreateTopicRequestData createTopicRequestData) {
    // Basic create with just topic name and partitions
    var partitionsCount = Optional.ofNullable(
        createTopicRequestData.getPartitionsCount()).orElse(1);
    var replicationFactor = Optional.ofNullable(
        createTopicRequestData.getReplicationFactor()).orElse(1).shortValue();
    return MutinyUtil.uniStage(
        adminClient.createTopics(List.of(new NewTopic(
            createTopicRequestData.getTopicName(),
            partitionsCount,
            replicationFactor
        ))).all().toCompletionStage())
        .chain(v -> Uni.createFrom().item(TopicData
            .builder()
            .kind("KafkaTopic")
            .topicName(createTopicRequestData.getTopicName())
            .clusterId(clusterId)
            .partitionsCount(partitionsCount)
            .replicationFactor((int) replicationFactor)
            .build()));
  }

  @Override
  public Uni<Void> deleteKafkaTopic(String clusterId, String topicName) {
    return MutinyUtil.uniStage(
        adminClient.deleteTopics(List.of(topicName)).all().toCompletionStage()
    ).chain(v -> Uni.createFrom().voidItem());
  }

  @Override
  public Uni<TopicData> getKafkaTopic(
      String clusterId, String topicName, Boolean includeAuthorizedOperations
  ) {
    var describeTopicsOptions = new DescribeTopicsOptions()
        .includeAuthorizedOperations(includeAuthorizedOperations);
    return MutinyUtil.uniStage(
        adminClient
            .describeTopics(List.of(topicName), describeTopicsOptions)
            .allTopicNames()
            .toCompletionStage()
    )
        .map(topicDescriptions -> topicDescriptions.values().iterator().next())
        .onItem().transform(this::fromTopicDescription);
  }

  @Override
  public Uni<TopicDataList> listKafkaTopics(String clusterId) {
    return MutinyUtil.uniStage(adminClient.listTopics().names().toCompletionStage())
        .chain(topicNames -> MutinyUtil.uniStage(
            adminClient.describeTopics(topicNames).allTopicNames().toCompletionStage()))
        .onItem()
        .transformToUni(topicDescriptionMap -> MutinyUtil.uniItem(TopicDataList
                .builder()
                .kind("KafkaTopicList")
                .data(topicDescriptionMap
                    .values()
                    .stream()
                    .map(this::fromTopicDescription)
                    .toList()
                ).build())
        );
  }

  @Override
  public Uni<TopicData> updatePartitionCountKafkaTopic(String clusterId, String topicName,
      UpdatePartitionCountRequestData updatePartitionCountRequestData) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private TopicData fromTopicDescription(TopicDescription topicDescription) {
    return TopicData
        .builder()
        .kind("KafkaTopic")
        .topicName(topicDescription.name())
        .clusterId(clusterId)
        .partitionsCount(topicDescription.partitions().size())
        .replicationFactor(topicDescription.partitions().getFirst().replicas().size())
        .isInternal(topicDescription.isInternal())
        .authorizedOperations(
            topicDescription.authorizedOperations().stream().map(Enum::name).toList()
        ).build();
  }

  @ServerExceptionMapper
  public Response mapUnknownTopicException(UnknownTopicOrPartitionException exception) {
    var error = Error
        .builder()
        .errorCode(Status.NOT_FOUND.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.NOT_FOUND)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapTopicAlreadyExistsException(TopicExistsException exception) {
    var error = Error
        .builder()
        .errorCode(Status.CONFLICT.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.CONFLICT)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }
}
