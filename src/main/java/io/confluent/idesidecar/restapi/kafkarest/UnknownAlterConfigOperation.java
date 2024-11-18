package io.confluent.idesidecar.restapi.kafkarest;

public class UnknownAlterConfigOperation extends IllegalArgumentException {
  public UnknownAlterConfigOperation(String operation) {
    super("Unknown alter config operation: " + operation);
  }
}
