package io.confluent.idesidecar.restapi.exceptions;

import io.confluent.idesidecar.restapi.models.ClusterType;

public class SchemaRegistryClusterNotFoundException extends ClusterNotFoundException {
  public SchemaRegistryClusterNotFoundException(String message) {
    super(message, ClusterType.SCHEMA_REGISTRY);
  }
}
