package io.confluent.idesidecar.restapi.kafkarest.impl;

import io.confluent.idesidecar.restapi.kafkarest.model.Relationship;
import io.confluent.idesidecar.restapi.util.Crn;
import org.eclipse.microprofile.config.ConfigProvider;

public final class RelationshipUtil {

  private static final String CRN_AUTHORITY = "";
  private static final String SIDECAR_HOST = ConfigProvider.getConfig().getValue(
      "ide-sidecar.api.host", String.class);

  public static Relationship forPartitions(String clusterId, String topicId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/topics/%s/partitions".formatted(
            SIDECAR_HOST, clusterId, topicId
        )).build();
  }

  public static Relationship forPartitionReassignments(String clusterId, String topicId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/topics/%s/partitions/-/reassignment".formatted(
            SIDECAR_HOST, clusterId, topicId
        )).build();
  }

  public static Relationship forAllPartitionReassignments(String clusterId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/topics/-/partitions/-/reassignment".formatted(
            SIDECAR_HOST, clusterId
        )).build();
  }

  public static Relationship forTopicConfigs(String clusterId, String topicId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/topics/%s/configs".formatted(
            SIDECAR_HOST, clusterId, topicId
        )).build();
  }

  public static Relationship forController(String clusterId, int brokerId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/brokers/%d".formatted(
            SIDECAR_HOST, clusterId, brokerId
        )).build();
  }

  public static Relationship forAcls(String clusterId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/acls".formatted(
            SIDECAR_HOST, clusterId
        )).build();
  }

  public static Relationship forBrokers(String clusterId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/brokers".formatted(
            SIDECAR_HOST, clusterId
        )).build();
  }

  public static Relationship forBrokerConfigs(String clusterId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/broker-configs".formatted(
            SIDECAR_HOST, clusterId
        )).build();
  }

  public static Relationship forConsumerGroups(String clusterId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/consumer-groups".formatted(
            SIDECAR_HOST, clusterId
        )).build();
  }

  public static Relationship forTopics(String clusterId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/topics".formatted(
            SIDECAR_HOST, clusterId
        )).build();
  }

  public static Relationship forTopic(String clusterId, String topicId) {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters/%s/topics/%s".formatted(
            SIDECAR_HOST, clusterId, topicId
        )).build();
  }

  public static Relationship forClusters() {
    return Relationship.builder().related(
        "%s/internal/kafka/v3/clusters".formatted(
            SIDECAR_HOST
        )).build();
  }

  public static Crn getClusterCrn(String clusterId) {
    return new Crn(
        CRN_AUTHORITY,
        Crn.newElements("kafka", clusterId),
        false
    );
  }

  public static Crn getTopicCrn(String clusterId, String topicName) {
    return new Crn(
        CRN_AUTHORITY,
        Crn.newElements("kafka", clusterId, "topic", topicName),
        false
    );
  }
}
