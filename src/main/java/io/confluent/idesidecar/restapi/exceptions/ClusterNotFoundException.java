package io.confluent.idesidecar.restapi.exceptions;

import io.confluent.idesidecar.restapi.models.ClusterType;

/**
 * Exception thrown when a requested Kafka or Schema Registry cluster cannot be found.
 */
public class ClusterNotFoundException extends Exception {

  private final ClusterType clusterType;

  public ClusterNotFoundException(String message, ClusterType clusterType) {
    super(message);
    this.clusterType = clusterType;
  }

  public ClusterType clusterType() {
    return clusterType;
  }
}
