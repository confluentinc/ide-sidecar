package io.confluent.idesidecar.restapi.util;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Testcontainers-based implementation of a local Confluent environment,
 * providing a single-node Kafka cluster with a REST Proxy.
 * This container is based on the {@code confluentinc/confluent-local} Docker image.
 *
 * <p>
 * This container is designed for integration testing of Kafka-based applications,
 * offering a lightweight, self-contained environment that includes:
 * <ul>
 *   <li>A single-node Kafka broker</li>
 *   <li>Kafka REST Proxy</li>
 * </ul>
 *
 * <p>
 * Internal network configuration:
 * <ul>
 *   <li>Container name and hostname: confluent-local-broker-1</li>
 *   <li>Internal Kafka listener: PLAINTEXT://confluent-local-broker-1:29092</li>
 * </ul>
 *
 * <p>
 * Exposed services:
 * <ul>
 *   <li>Kafka: Internal port 9092, mapped to a random port on the host</li>
 *   <li>REST Proxy: Internal port 8082, mapped to a random port on the host</li>
 * </ul>
 *
 * <p>
 * Usage guidelines:
 * <ul>
 *   <li>For communication within the Docker network (e.g., with SchemaRegistry),
 *       use the internal listener address.</li>
 *   <li>For external connections (e.g., from test code), use the provided methods
 *       {@code getKafkaBootstrapServers()} and {@code getRestProxyEndpoint()}
 *       to obtain the appropriate connection strings.</li>
 * </ul>
 *
 * <p>
 * This container is typically used in combination with other Testcontainers
 * to create a comprehensive local testing environment for Kafka-based applications.
 * It's particularly useful for scenarios where you need to test both Kafka
 * producers/consumers and RESTful interactions with Kafka.
 *
 * <p>
 * Example usage:
 * <pre>
 * {@code
 * try (ConfluentLocalKafkaWithRestProxyContainer container = new ConfluentLocalKafkaWithRestProxyContainer()) {
 *     container.start();
 *     String bootstrapServers = container.getKafkaBootstrapServers();
 *     String restProxyUrl = container.getRestProxyEndpoint();
 *     // Use bootstrapServers and restProxyUrl in tests
 * }
 * }
 * </pre>
 */
public class ConfluentLocalKafkaWithRestProxyContainer
    extends GenericContainer<ConfluentLocalKafkaWithRestProxyContainer>
    implements AutoCloseable {
  private static final int KAFKA_PORT = 9092;
  private static final String DEFAULT_IMAGE = "confluentinc/confluent-local:7.6.0";
  private static final String CONTAINER_NAME = "confluent-local-broker-1";
  private static final String REST_PROXY_HOST_NAME = "rest-proxy";
  public static final int REST_PROXY_PORT = 8082;
  public static final String CLUSTER_ID = "oh-sxaDRTcyAr6pFRbXyzA";

  public ConfluentLocalKafkaWithRestProxyContainer() {
    this(DEFAULT_IMAGE);
  }

  public ConfluentLocalKafkaWithRestProxyContainer(String dockerImageName) {
    super(DockerImageName.parse(dockerImageName));
    super.withEnv(getEnvironmentVariables())
        .withExposedPorts(KAFKA_PORT, REST_PROXY_PORT)
        .withCreateContainerCmdModifier(cmd -> cmd
            .withName(CONTAINER_NAME)
            .withHostName(CONTAINER_NAME));
    setPortBindings(List.of(
        String.format("%d:%d", REST_PROXY_PORT, REST_PROXY_PORT),
        String.format("%d:%d", KAFKA_PORT, KAFKA_PORT)
    ));
  }

  public String getClusterId() {
    return CLUSTER_ID;
  }

  public String getKafkaBootstrapServers() {
    return String.format("%s:%d", getHost(), getMappedPort(KAFKA_PORT));
  }

  public String getRestProxyEndpoint() {
    return String.format("http://%s:%d", getHost(), getMappedPort(REST_PROXY_PORT));
  }

  private Map<String, String> getEnvironmentVariables() {
    Map<String, String> env = new HashMap<>();
    env.put("CLUSTER_ID", CLUSTER_ID);
    env.put(
        "KAFKA_CONTROLLER_QUORUM_VOTERS",
        String.format("1@%s:29093", CONTAINER_NAME)
    );
    env.put(
        "KAFKA_LISTENERS",
        String.format("PLAINTEXT://%s:29092,CONTROLLER://%s:29093,PLAINTEXT_HOST://0.0.0.0:%d",
            CONTAINER_NAME,
            CONTAINER_NAME,
            KAFKA_PORT)
    );
    env.put(
        "KAFKA_ADVERTISED_LISTENERS",
        String.format("PLAINTEXT://%s:29092,PLAINTEXT_HOST://localhost:%d",
            CONTAINER_NAME,
            KAFKA_PORT)
    );
    env.put("KAFKA_REST_HOST_NAME", REST_PROXY_HOST_NAME);
    env.put("KAFKA_REST_LISTENERS",
        String.format("http://0.0.0.0:%d", REST_PROXY_PORT)
    );
    env.put("KAFKA_REST_BOOTSTRAP_SERVERS",
        String.format("%s:29092", CONTAINER_NAME)
    );
    return env;
  }

  @Override
  public void close() {
    super.stop();
  }
}