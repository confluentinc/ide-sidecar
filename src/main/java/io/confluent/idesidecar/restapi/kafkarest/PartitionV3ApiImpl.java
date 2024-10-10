package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.api.PartitionV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.PartitionData;
import io.confluent.idesidecar.restapi.kafkarest.model.PartitionDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ReassignmentData;
import io.confluent.idesidecar.restapi.kafkarest.model.ReassignmentDataList;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;

@RequestScoped
public class PartitionV3ApiImpl implements PartitionV3Api {

  @Inject
  PartitionManagerImpl partitionManager;

  @Override
  public Uni<PartitionData> getKafkaPartition(String clusterId, String topicName, Integer partitionId) {
    return partitionManager.getKafkaPartition(clusterId, topicName, partitionId);
  }

  @Override
  public Uni<PartitionDataList> listKafkaPartitions(String clusterId, String topicName) {
    return partitionManager.listKafkaPartitions(clusterId, topicName);
  }

  @Override
  public Uni<ReassignmentDataList> internalKafkaV3ClustersClusterIdTopicsPartitionsReassignmentGet(String clusterId) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Uni<ReassignmentData> internalKafkaV3ClustersClusterIdTopicsTopicNamePartitionsPartitionIdReassignmentGet(String clusterId, String topicName, Integer partitionId) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Uni<ReassignmentDataList> internalKafkaV3ClustersClusterIdTopicsTopicNamePartitionsReassignmentGet(String clusterId, String topicName) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
