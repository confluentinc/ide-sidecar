package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.Relationship;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Utility class for generating {@code related} links for Kafka resources.
 */
public final class RelationshipUtil {

  private RelationshipUtil() {
  }

  private static final String SIDECAR_HOST = ConfigProvider.getConfig().getValue(
      "ide-sidecar.api.host", String.class);

  public static Relationship forPartitions(String clusterId, String topicId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/topics/%s/partitions".formatted(
        SIDECAR_HOST, clusterId, topicId
    ));
  }

  public static Relationship forPartitionReassignments(String clusterId, String topicId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/topics/%s/partitions/-/reassignment"
        .formatted(
            SIDECAR_HOST, clusterId, topicId
        ));
  }

  public static Relationship forAllPartitionReassignments(String clusterId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/topics/-/partitions/-/reassignment"
        .formatted(
            SIDECAR_HOST, clusterId
        ));
  }

  public static Relationship forTopicConfigs(String clusterId, String topicId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/topics/%s/configs".formatted(
        SIDECAR_HOST, clusterId, topicId
    ));
  }

  public static Relationship forController(String clusterId, int brokerId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/brokers/%d".formatted(
        SIDECAR_HOST, clusterId, brokerId
    ));
  }

  public static Relationship forAcls(String clusterId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/acls".formatted(
        SIDECAR_HOST, clusterId
    ));
  }

  public static Relationship forBrokers(String clusterId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/brokers".formatted(
        SIDECAR_HOST, clusterId
    ));
  }

  public static Relationship forBrokerConfigs(String clusterId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/broker-configs".formatted(
        SIDECAR_HOST, clusterId
    ));
  }

  public static Relationship forConsumerGroups(String clusterId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/consumer-groups".formatted(
        SIDECAR_HOST, clusterId
    ));
  }

  public static Relationship forTopics(String clusterId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/topics".formatted(
        SIDECAR_HOST, clusterId
    ));
  }

  public static Relationship forTopic(String clusterId, String topicId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/topics/%s".formatted(
        SIDECAR_HOST, clusterId, topicId
    ));
  }

  public static Relationship forClusters() {
    return createRelationship("%s/internal/kafka/v3/clusters".formatted(
        SIDECAR_HOST
    ));
  }

  public static Relationship forCluster(String clusterId) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s".formatted(
        SIDECAR_HOST, clusterId
    ));
  }

  public static Relationship forPartition(
      String clusterId, String topicName, Integer partitionId
  ) {
    return createRelationship("%s/internal/kafka/v3/clusters/%s/topics/%s/partitions/%d".formatted(
        SIDECAR_HOST, clusterId, topicName, partitionId));
  }

  public static Relationship forTopicPartitions(
      String clusterId, String topicName
  ) {
    return createRelationship("%s/v3/clusters/%s/topics/%s/partitions".formatted(
        SIDECAR_HOST, clusterId, topicName));
  }

  public static Relationship forLeader(
      String clusterId, String topicName, Integer partitionId, Integer leaderId
  ) {
    return createRelationship("/v3/clusters/%s/topics/%s/partitions/%d/replicas/%d"
        .formatted(clusterId, topicName, partitionId, leaderId));
  }

  public static Relationship forReplicas(
      String clusterId, String topicName, Integer partitionId
  ) {
    return createRelationship("/v3/clusters/%s/topics/%s/partitions/%d/replicas"
        .formatted(clusterId, topicName, partitionId));
  }

  public static Relationship forReassignment(
      String clusterId,
      String topicName,
      Integer partitionId
  ) {
    return createRelationship("/v3/clusters/%s/topics/%s/partitions/%d/reassignment"
        .formatted(clusterId, topicName, partitionId));
  }

  private static Relationship createRelationship(String related) {
    return Relationship.builder().related(related).build();
  }
}
