package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.credentials.TLSConfigBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpecKafkaClusterConfigBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpecSchemaRegistryConfigBuilder;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.TestProfile;
import java.time.Duration;
import java.util.Optional;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * A {@link TestEnvironment} that starts a local Confluent Local container with Kafka broker and
 * Kafka REST server, and a Confluent Platform Schema Registry container.
 */
@TestProfile(NoAccessFilterProfile.class)
public class LocalTestEnvironment implements TestEnvironment {

  private static final String KAFKA_INTERNAL_LISTENER = "PLAINTEXT://confluent-local-broker-1:29092";

  private Network network;
  private ConfluentLocalKafkaWithRestProxyContainer kafkaWithRestProxy;
  private SchemaRegistryContainer schemaRegistry;

  public LocalTestEnvironment() {
  }

  @Override
  public void start() {
    createNetworkAndContainers();

    startContainers();
  }

  @Override
  public void shutdown() {
    close();
  }

  protected void createNetworkAndContainers() {
    network = Network.newNetwork();
    kafkaWithRestProxy = new ConfluentLocalKafkaWithRestProxyContainer()
        .withNetwork(network)
        .withNetworkAliases("kafka")
        .waitingFor(Wait.forLogMessage(
            ".*Server started, listening for requests.*\\n", 1))
        // Kafka REST server port
        .waitingFor(Wait.forListeningPorts(
            ConfluentLocalKafkaWithRestProxyContainer.REST_PROXY_PORT
        ));

    schemaRegistry = new SchemaRegistryContainer(KAFKA_INTERNAL_LISTENER)
        .withNetwork(network)
        .withExposedPorts(8081)
        .withNetworkAliases("schema-registry")
        .dependsOn(kafkaWithRestProxy)
        .waitingFor(
            Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
  }

  protected void startContainers() {
    kafkaWithRestProxy.start(); // start first
    schemaRegistry.start();
  }

  protected void stopContainers() {
    schemaRegistry.stop(); // stop first
    kafkaWithRestProxy.stop();
  }

  public void close() {
    stopContainers();
    network.close();
  }

  public Optional<ConnectionSpec> localConnectionSpec() {
    return Optional.of(
        ConnectionSpec.createLocalWithSRConfig(
            "local-connection",
            "Local",
            new ConnectionSpec.SchemaRegistryConfig(
                "local-schema-registry",
                schemaRegistry.endpoint(),
                null,
                // Disable TLS
                TLSConfigBuilder.builder().enabled(false).build()
            )
        )
    );
  }

  public Optional<ConnectionSpec> directConnectionSpec() {
    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection",
            "Direct to Local",
            ConnectionSpecKafkaClusterConfigBuilder
                .builder()
                .bootstrapServers(kafkaWithRestProxy.getKafkaBootstrapServers())
                // Disable TLS
                .tlsConfig(TLSConfigBuilder.builder().enabled(false).build())
                .build(),
            ConnectionSpecSchemaRegistryConfigBuilder
                .builder()
                .id(schemaRegistry.getClusterId())
                .uri(schemaRegistry.endpoint())
                // Disable TLS
                .tlsConfig(TLSConfigBuilder.builder().enabled(false).build())
                .build()
        )
    );
  }
}