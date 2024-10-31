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

  public static final LocalTestEnvironment INSTANCE = new LocalTestEnvironment();

  private static final String KAFKA_INTERNAL_LISTENER = "PLAINTEXT://confluent-local-broker-1:29092";

  private Network network;
  private ConfluentLocalKafkaWithRestProxyContainer kafkaWithRestProxy;
  private SchemaRegistryContainer schemaRegistry;
  private final Set<String> joinedTestClasses = new HashSet<>();
  private final AtomicBoolean started = new AtomicBoolean();

  public LocalTestEnvironment() {
  }

  @Override
  public void start() {
    createNetworkAndContainers();

    startContainers();
  }

  @Override
  public void shutdown() {
    stopContainers();
  }

  @Override
  public synchronized void join(String testClassName) {
    joinedTestClasses.add(testClassName);
    if (started.compareAndSet(false, true)) {
      start();
    }
  }

  @Override
  public synchronized void leave(String testClassName) {
    joinedTestClasses.remove(testClassName);
    if (joinedTestClasses.isEmpty()) {
      shutdown();
    }
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

  @Override
  public void close() {
    schemaRegistry.close();
    kafkaWithRestProxy.close();
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
                "local-kafka",
                kafkaWithRestProxy.getKafkaBootstrapServers()
            ),
            new ConnectionSpec.SchemaRegistryConfig(
                "local-schema-registry",
                schemaRegistry.endpoint()
            )
        )
    );
  }
}