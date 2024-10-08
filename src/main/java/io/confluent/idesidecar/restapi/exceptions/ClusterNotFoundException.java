package io.confluent.idesidecar.restapi.exceptions;

/**
 * Exception thrown when a requested Kafka or Schema Registry cluster cannot be found.
 */
public class ClusterNotFoundException extends RuntimeException {

  public ClusterNotFoundException() {
    super("Cluster not found.");
  }

  public ClusterNotFoundException(String message) {
    super(message);
  }

  public ClusterNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClusterNotFoundException(Throwable cause) {
    super(cause);
  }
}
