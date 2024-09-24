package io.confluent.idesidecar.restapi.util;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

  private static final int PORT = 8081;

  public SchemaRegistryContainer(String kafkaBootstrap) {
    this("confluentinc/cp-schema-registry:7.7.0", kafkaBootstrap);
  }

  public SchemaRegistryContainer(String dockerImageName, String kafkaBootstrap) {
    super(DockerImageName.parse(dockerImageName));
    withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaBootstrap);
    withEnv("SCHEMA_REGISTRY_HOST_NAME", "schemaRegistry");
    withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + PORT);
    // This will map container's port 8081 to host's port 8081
    addFixedExposedPort(PORT, PORT);
  }

  public String endpoint() {
    return String.format("http://%s:%d", getHost(), getMappedPort(PORT));
  }
}
