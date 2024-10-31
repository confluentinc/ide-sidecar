/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.util;

import static io.confluent.idesidecar.restapi.kafkarest.SchemaManager.SCHEMA_PROVIDERS;

import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumerIT;
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

  private static final LocalTestEnvironment ENV = LocalTestEnvironment.INSTANCE;

  protected SimpleConsumer simpleConsumer;

  protected SidecarClient.KafkaCluster kafkaCluster;
  protected SidecarClient.SchemaRegistry srCluster;

  @BeforeAll
  public static void beforeAll() {
    ENV.join(SimpleConsumerIT.class);
  }

  @AfterAll
  public static void afterAll() {
    ENV.leave(SimpleConsumerIT.class);
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

  protected void setupLocalConnection() {
    // Create the local connection we'll use
    var localConnectionId = createLocalConnectionTo(ENV).id();
    useConnection(localConnectionId);

    // Get the clusters we'll use
    kafkaCluster = getKafkaCluster().orElseThrow();
    srCluster = getSchemaRegistryCluster().orElseThrow();
    useClusters(kafkaCluster, srCluster);

    // And create a simple consumer
    simpleConsumer = createSimpleConsumer(kafkaCluster, srCluster);
  }

  protected void setupDirectConnection() {
    // Create the direct connection we'll use
    var directConnectionId = createDirectConnectionTo(ENV).id();
    useConnection(directConnectionId);

    // Get the clusters we'll use
    kafkaCluster = getKafkaCluster().orElseThrow();
    srCluster = getSchemaRegistryCluster().orElseThrow();
    useClusters(kafkaCluster, srCluster);

    // And create a simple consumer
    simpleConsumer = createSimpleConsumer(kafkaCluster, srCluster);
  }
}
