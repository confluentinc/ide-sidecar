/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.util;

import static io.confluent.idesidecar.restapi.kafkarest.SchemaManager.SCHEMA_PROVIDERS;

import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

public abstract class AbstractSidecarIT extends SidecarClient {

  /**
   * Use the <a href="https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers">Singleton Container</a>
   * pattern to ensure that the test environment is only started once, no matter how many
   * test classes extend this class. Testcontainers will assure that this is initialized once,
   * and stop the containers using the Ryuk container after all the tests have run.
   */
  private static final LocalTestEnvironment TEST_ENVIRONMENT = new LocalTestEnvironment();

  protected SimpleConsumer simpleConsumer;

  protected SidecarClient.KafkaCluster kafkaCluster;
  protected SidecarClient.SchemaRegistry srCluster;

  @BeforeAll
  public static void beforeAll() {
    TEST_ENVIRONMENT.start();
  }

  @AfterAll
  public static void afterAll() {
    // This may not stop the container, and instead may just mark it for stopping while letting
    // Ryuk container handle the actual stopping. This is because the container is a singleton.
    TEST_ENVIRONMENT.shutdown();
  }

  @AfterEach
  public void afterEach() {
    deleteAllContent();
  }

  protected SimpleConsumer createSimpleConsumer(
      SidecarClient.KafkaCluster kafkaCluster,
      SidecarClient.SchemaRegistry srCluster
  ) {
    // Setup a simple consumer
    var consumerProps = new Properties();
    var sidecarHost = sidecarHost();
    consumerProps.setProperty("bootstrap.servers", kafkaCluster.bootstrapServers());
    consumerProps.setProperty("schema.registry.url", srCluster.uri());
    return new SimpleConsumer(
        new KafkaConsumer<>(
            consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer()
        ),
        new CachedSchemaRegistryClient(
            Collections.singletonList(sidecarHost),
            10,
            SCHEMA_PROVIDERS,
            Collections.emptyMap(),
            Map.of(
                RequestHeadersConstants.CONNECTION_ID_HEADER, currentConnectionId(),
                RequestHeadersConstants.CLUSTER_ID_HEADER, srCluster.id()
            )
        ),
        new RecordDeserializer()
    );
  }

  /**
   * Test classes that extend {@link AbstractSidecarIT} should call this method in their
   * {@code @BeforeEach} method to set up the local connection the test methods will use.
   *
   * @see #setupDirectConnection()
   */
  protected void setupLocalConnection() {
    // Create the local connection we'll use
    var localConnectionId = createLocalConnectionTo(TEST_ENVIRONMENT).id();
    useConnection(localConnectionId);

    // Get the clusters we'll use
    kafkaCluster = getKafkaCluster().orElseThrow();
    srCluster = getSchemaRegistryCluster().orElseThrow();
    useClusters(kafkaCluster, srCluster);

    // Set the current cluster to the Kafka cluster (by default)
    setCurrentCluster(kafkaCluster.id());

    // And create a simple consumer
    simpleConsumer = createSimpleConsumer(kafkaCluster, srCluster);
  }

  /**
   * Test classes that extend {@link AbstractSidecarIT} should call this method in their
   * {@code @BeforeEach} method to set up the direct connection the test methods will use.
   *
   * @see #setupLocalConnection()
   */
  protected void setupDirectConnection() {
    // Create the direct connection we'll use
    var directConnectionId = createDirectConnectionTo(TEST_ENVIRONMENT).id();
    useConnection(directConnectionId);

    // Get the clusters we'll use
    kafkaCluster = getKafkaCluster().orElseThrow();
    srCluster = getSchemaRegistryCluster().orElseThrow();
    useClusters(kafkaCluster, srCluster);

    // Set the current cluster to the Kafka cluster (by default)
    setCurrentCluster(kafkaCluster.id());

    // And create a simple consumer
    simpleConsumer = createSimpleConsumer(kafkaCluster, srCluster);
  }
}
