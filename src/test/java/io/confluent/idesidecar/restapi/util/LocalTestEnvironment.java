package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.TestProfile;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

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
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
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
        ConnectionSpec.createLocal(
            "local-connection",
            "Local",
            new ConnectionSpec.LocalConfig(
                schemaRegistry.endpoint()
            )
        )
    );
  }

  public Optional<ConnectionSpec> directConnectionSpec() {
    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection",
            "Direct to Local",
            new ConnectionSpec.KafkaClusterConfig(
                kafkaWithRestProxy.getClusterId(),
                kafkaWithRestProxy.getKafkaBootstrapServers(),
                null,
                null,
                null
            ),
            new ConnectionSpec.SchemaRegistryConfig(
                schemaRegistry.getClusterId(),
                schemaRegistry.endpoint(),
                null
            )
        )
    );
  }
}