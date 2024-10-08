package io.confluent.idesidecar.restapi.kafkarest.exceptions;

/**
 * Exception thrown when an AdminClient cannot be instantiated. This can happen if the AdminClient
 * cannot connect to the Kafka cluster, or if we could not determine the AdminClient configuration.
 */
public class AdminClientInstantiationException extends RuntimeException {
  public AdminClientInstantiationException(String message) {
    super(message);
  }
}
