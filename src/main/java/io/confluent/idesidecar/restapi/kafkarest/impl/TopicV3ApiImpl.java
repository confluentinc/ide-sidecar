package io.confluent.idesidecar.restapi.kafkarest.impl;

import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forPartitionReassignments;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forPartitions;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forTopic;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forTopicConfigs;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.forTopics;
import static io.confluent.idesidecar.restapi.kafkarest.impl.RelationshipUtil.getTopicCrn;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import io.confluent.idesidecar.restapi.kafkarest.api.TopicV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.Error;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.UpdatePartitionCountRequestData;
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
import java.util.Set;
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
    // TODO: Support creating topic using all CreateTopicRequestData fields
    return uniStage(
        adminClient.createTopics(List.of(new NewTopic(
            createTopicRequestData.getTopicName(),
            Optional.ofNullable(createTopicRequestData.getPartitionsCount())
                .orElse(1),
            Optional.ofNullable(createTopicRequestData.getReplicationFactor())
                .orElse(1).shortValue()
        ))).all().toCompletionStage())
        .chain(v -> getKafkaTopic(clusterId, createTopicRequestData.getTopicName(), false));
  }

  @Override
  public Uni<Void> deleteKafkaTopic(String clusterId, String topicName) {
    return uniStage(adminClient.deleteTopics(List.of(topicName)).all().toCompletionStage());
  }

  @Override
  public Uni<TopicData> getKafkaTopic(
      String clusterId, String topicName, Boolean includeAuthorizedOperations
  ) {
    var describeTopicsOptions = new DescribeTopicsOptions()
        .includeAuthorizedOperations(
            Optional.ofNullable(includeAuthorizedOperations).orElse(false)
        );
    return uniStage(
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
    return uniStage(adminClient.listTopics().names().toCompletionStage())
        .chain(topicNames -> uniStage(
            adminClient.describeTopics(topicNames).allTopicNames().toCompletionStage()))
        .onItem()
        .transformToUni(topicDescriptionMap -> uniItem(TopicDataList
                .builder()
                .kind("KafkaTopicList")
                .metadata(ResourceCollectionMetadata
                    .builder()
                    .next(null)
                    .self(forTopics(clusterId).getRelated())
                    .build()
                )
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
            Optional.ofNullable(topicDescription.authorizedOperations()).orElse(Set.of())
                .stream().map(Enum::name).toList()
        )
        .partitionReassignments(forPartitionReassignments(clusterId, topicDescription.name()))
        .partitions(forPartitions(clusterId, topicDescription.name()))
        .configs(forTopicConfigs(clusterId, topicDescription.name()))
        .metadata(getTopicMetadata(clusterId, topicDescription.name())).build();
  }

  private ResourceMetadata getTopicMetadata(String clusterId, String topicName) {
    return ResourceMetadata
        .builder()
        .resourceName(getTopicCrn(clusterId, topicName).toString())
        .self(forTopic(clusterId, topicName).getRelated())
        .build();
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
