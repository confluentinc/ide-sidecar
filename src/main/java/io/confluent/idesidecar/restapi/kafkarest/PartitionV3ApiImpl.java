package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forLeader;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forPartition;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forReassignment;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forReplicas;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forTopicPartitions;

import io.confluent.idesidecar.restapi.kafkarest.api.PartitionV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.PartitionData;
import io.confluent.idesidecar.restapi.kafkarest.model.PartitionDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ReassignmentData;
import io.confluent.idesidecar.restapi.kafkarest.model.ReassignmentDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.kafka.common.TopicPartitionInfo;

@RequestScoped
public class PartitionV3ApiImpl implements PartitionV3Api {

  @Inject
  PartitionManager partitionManager;

  @Override
  public Uni<PartitionData> getKafkaPartition(
      String clusterId, String topicName, Integer partitionId
  ) {
    return partitionManager
        .getKafkaPartition(clusterId, topicName, partitionId)
        .onItem()
        .transform(partitionInfo -> getPartitionData(clusterId, topicName, partitionInfo));
  }

  @Override
  public Uni<PartitionDataList> listKafkaPartitions(String clusterId, String topicName) {
    return partitionManager
        .listKafkaPartitions(clusterId, topicName)
        .onItem()
        .transform(partitionInfos ->
            PartitionDataList.builder()
                .kind("KafkaPartitionList")
                .metadata(ResourceCollectionMetadata
                    .builder()
                    .self(forTopicPartitions(clusterId, topicName).getRelated())
                    // We don't support pagination
                    .next(null)
                    .build()
                )
                .data(partitionInfos
                    .stream()
                    .map(partitionInfo -> getPartitionData(clusterId, topicName, partitionInfo))
                    .toList()
                ).build()
        );
  }

  private static PartitionData getPartitionData(
      String clusterId, String topicName, TopicPartitionInfo partitionInfo
  ) {
    return PartitionData.builder()
        .kind("KafkaPartition")
        .metadata(
            ResourceMetadata.builder()
                .self(forPartition(clusterId, topicName, partitionInfo.partition()).getRelated())
                // TODO: Construct resource name based on the connection/cluster type
                .resourceName(null)
                .build()
        )
        .clusterId(clusterId)
        .partitionId(partitionInfo.partition())
        .topicName(topicName)
        .leader(forLeader(
            clusterId, topicName, partitionInfo.partition(), partitionInfo.leader().id()))
        .replicas(forReplicas(
            clusterId, topicName, partitionInfo.partition()))
        .reassignment(forReassignment(
            clusterId, topicName, partitionInfo.partition()))
        .build();
  }

  @Override
  public Uni<ReassignmentDataList> internalKafkaV3ClustersClusterIdTopicsPartitionsReassignmentGet(
      String clusterId
  ) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Uni<ReassignmentData>
  internalKafkaV3ClustersClusterIdTopicsTopicNamePartitionsPartitionIdReassignmentGet(
      String clusterId, String topicName, Integer partitionId
  ) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Uni<ReassignmentDataList>
  internalKafkaV3ClustersClusterIdTopicsTopicNamePartitionsReassignmentGet(
      String clusterId, String topicName
  ) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
