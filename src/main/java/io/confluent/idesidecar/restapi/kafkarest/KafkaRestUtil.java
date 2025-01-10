package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.models.ClusterType;

public class KafkaRestUtil {

  public static String constructResourceName(ClusterType clusterType, String clusterId, String topicName) {
    String authority = switch (clusterType) {
      case KAFKA -> "kafka";
      case SCHEMA_REGISTRY -> "schema-registry";
      default -> "unknown";
    };
    return "crn://" + authority + "/resource=" + clusterId + "/sub-resource=" + topicName;
  }

  public static String constructResourceName(ClusterType clusterType, String clusterId) {
    String authority = switch (clusterType) {
      case KAFKA -> "kafka";
      case SCHEMA_REGISTRY -> "schema-registry";
      default -> "unknown";
    };
    return "crn://" + authority + "/resource=" + clusterId;
  }

}
