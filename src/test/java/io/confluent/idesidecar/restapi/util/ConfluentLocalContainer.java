package io.confluent.idesidecar.restapi.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainers-based implementation for Confluent Local. Exposes a single-broker Kafka cluster
 * that uses KRaft and a Kafka REST Proxy instance. Uses the Docker image:
 * {@code confluentinc/confluent-local}
 *
 * <p>
 *   Exposed ports:
 *   <ul>
 *     <li>Kafka: 9092</li>
 *     <li>REST Proxy: 8082</li>
 *   </ul>
 */
public class ConfluentLocalContainer implements AutoCloseable {

  static final DockerImageName DEFAULT_IMAGE_NAME =
      DockerImageName.parse("confluentinc/confluent-local");
  static final String DEFAULT_TAG = "7.6.0";
  static final String DEFAULT_KAFKA_CONTAINER_NAME = "confluent-local-broker-1";
  static final Integer DEFAULT_KAFKA_HOST_PLAINTEXT_PORT = 9092;
  static final Integer DEFAULT_KAFKA_REST_PROXY_PORT = 8082;
  static final String DEFAULT_KAFKA_REST_PROXY_HOSTNAME = "rest-proxy";
  static final String DEFAULT_CLUSTER_ID = "oh-sxaDRTcyAr6pFRbXyzA";

  final GenericContainer testContainer;

  public ConfluentLocalContainer() {
    this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
  }

  public ConfluentLocalContainer(final DockerImageName dockerImageName) {
    dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);

    testContainer = new GenericContainer<>(dockerImageName)
        .withEnv(getEnv())
        .withExposedPorts(DEFAULT_KAFKA_REST_PROXY_PORT, DEFAULT_KAFKA_HOST_PLAINTEXT_PORT)
        .withCreateContainerCmdModifier(it -> it
            .withName(DEFAULT_KAFKA_CONTAINER_NAME)
            .withHostName(DEFAULT_KAFKA_CONTAINER_NAME)
        );

    testContainer.setPortBindings(
        List.of(
            String.format(
                "%d:%d",
                DEFAULT_KAFKA_REST_PROXY_PORT,
                DEFAULT_KAFKA_REST_PROXY_PORT
            ),
            String.format(
                "%d:%d",
                DEFAULT_KAFKA_HOST_PLAINTEXT_PORT,
                DEFAULT_KAFKA_HOST_PLAINTEXT_PORT
            )
        )
    );
  }

  /**
   * Starts the testcontainer.
   */
  public void start() {
    testContainer.start();
  }

  /**
   * Stops the testcontainer.
   */
  public void close() {
    testContainer.close();
  }

  Map<String, String> getEnv() {
    return new HashMap<>() {{
      put("CLUSTER_ID", DEFAULT_CLUSTER_ID);
      put(
          "KAFKA_CONTROLLER_QUORUM_VOTERS",
          String.format("1@%s:29093", DEFAULT_KAFKA_CONTAINER_NAME)
      );
      put("KAFKA_LISTENERS",
          String.format(
              "PLAINTEXT://%s:29092,CONTROLLER://%s:29093,PLAINTEXT_HOST://0.0.0.0:%d",
              DEFAULT_KAFKA_CONTAINER_NAME,
              DEFAULT_KAFKA_CONTAINER_NAME,
              DEFAULT_KAFKA_HOST_PLAINTEXT_PORT
          )
      );
      put(
          "KAFKA_ADVERTISED_LISTENERS",
          String.format(
              "PLAINTEXT://%s:29092,PLAINTEXT_HOST://localhost:%d",
              DEFAULT_KAFKA_CONTAINER_NAME,
              DEFAULT_KAFKA_HOST_PLAINTEXT_PORT
          )
      );
      put("KAFKA_REST_HOST_NAME", DEFAULT_KAFKA_REST_PROXY_HOSTNAME);
      put(
          "KAFKA_REST_LISTENERS",
          String.format("http://0.0.0.0:%d", DEFAULT_KAFKA_REST_PROXY_PORT)
      );
      put("KAFKA_REST_BOOTSTRAP_SERVERS", String.format("%s:29092", DEFAULT_KAFKA_CONTAINER_NAME));
    }};
  }
}
