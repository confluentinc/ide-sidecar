package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.fail;

import com.github.dockerjava.api.model.Container;
import io.confluent.idesidecar.restapi.credentials.*;
import io.confluent.idesidecar.restapi.credentials.ScramCredentials.HashAlgorithm;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpecKafkaClusterConfigBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpecSchemaRegistryConfigBuilder;
import io.confluent.idesidecar.restapi.util.cpdemo.CPServerContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.OpenldapContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.SchemaRegistryContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.ToolsContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.ZookeeperContainer;
import io.confluent.idesidecar.restapi.credentials.Credentials;
import io.quarkus.logging.Log;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.TestcontainersConfiguration;

/**
 * A {@link TestEnvironment} that starts a CP Demo environment with a two-node Kafka cluster,
 * Zookeeper, OpenLDAP, and Schema Registry.
 * Modeled after https://github.com/confluentinc/cp-demo/blob/7.7.1-post/docker-compose.yml
 */
public class CPDemoTestEnvironment implements TestEnvironment {
  private Network network;
  private ToolsContainer tools;
  private ZookeeperContainer zookeeper;
  private OpenldapContainer ldap;
  private CPServerContainer kafka1;
  private CPServerContainer kafka2;
  private SchemaRegistryContainer schemaRegistry;

  private static final List<String> CP_DEMO_CONTAINERS = List.of(
      "tools", "zookeeper", "kafka1", "kafka2", "openldap", "schemaregistry"
  );

  @Override
  public void start() {
    // If we see that some but not all cp-demo containers are in running state,
    // complain and exit.
    var cpDemoRunning = isCpDemoRunningAllContainers();
    if (isCpDemoRunningAnyContainer() && !cpDemoRunning) {
      fail("Detected some but not all cp-demo containers running. "
          + "Please stop all cp-demo containers using make cp-demo-stop and try running the tests again.");
    }

    // If we see that all cp-demo containers are exited, remove them.
    removeCPDemoContainersIfStopped();

    // Run the setup script
    runScript("src/test/resources/cp-demo-scripts/setup.sh");

    network = createReusableNetwork("cp-demo");
    // Check if zookeeper, kafka1, kafka2, ldap, schemaRegistry are already running
    Log.info("Starting Tools...");
    tools = new ToolsContainer(network);
    tools.start();

    if (!cpDemoRunning) {
      Log.info("Registering root CA...");
      registerRootCA();
    }

    Log.info("Starting Zookeeper...");
    zookeeper = new ZookeeperContainer(network);
    zookeeper.waitingFor(Wait.forHealthcheck());
    zookeeper.start();

    Log.info("Starting OpenLDAP...");
    ldap = new OpenldapContainer(network);
    ldap.start();

    kafka1 = new CPServerContainer(
        network,
        "kafka1",
        8091,
        9091,
        10091,
        11091,
        12091,
        12093,
        13091,
        14091,
        15091
    );
    kafka1.withEnv(Map.of(
        "KAFKA_BROKER_ID", "1",
        "KAFKA_BROKER_RACK", "r1",
        "KAFKA_JMX_PORT", "9991"
    ));
    kafka2 = new CPServerContainer(
        network,
        "kafka2",
        8092,
        9092,
        10092,
        11092,
        12092,
        12094,
        13092,
        14092,
        15092
    );
    kafka2.withEnv(Map.of(
        "KAFKA_BROKER_ID", "2",
        "KAFKA_BROKER_RACK", "r2",
        "KAFKA_JMX_PORT", "9992"
    ));

    // Must be started in parallel
    Log.info("Starting Kafka brokers...");
    Startables.deepStart(List.of(kafka1, kafka2)).join();

    // Register users for SASL/SCRAM
    registerScramUsers();

    if (!cpDemoRunning) {
      Log.info("Creating role bindings...");
      runToolScript("/tmp/helper/create-role-bindings.sh");
      setMinISR();
    }

    Log.info("Starting Schema Registry...");
    schemaRegistry = new SchemaRegistryContainer(network);
    schemaRegistry.start();
  }

  /**
   * We don't stop the containers after tests are run. This is used to stop the containers manually
   * from the {@link #main(String[])} method. Refer to the Make target
   * {@code make cp-demo-stop} for stopping the cp-demo containers.
   */
  @Override
  public void shutdown() {
    shutdownContainers();
  }

  /**
   * Workaround for setting min ISR on topic _confluent-metadata-auth
   */
  private void setMinISR() {
    try {
      kafka1.execInContainer(
          "kafka-configs",
          "--bootstrap-server", "kafka1:12093",
          "--entity-type", "topics",
          "--entity-name", "_confluent-metadata-auth",
          "--alter",
          "--add-config", "min.insync.replicas=1"
      );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void registerScramUsers() {
    Log.info("Creating users for SASL/SCRAM authentication.");
    try {
      kafka1.execInContainer(
          "kafka-configs",
          "--bootstrap-server", "kafka1:12093",
          "--entity-type", "users",
          "--entity-name", "admin",
          "--alter",
          "--add-config", "SCRAM-SHA-256=[password=admin-secret]"
      );
    } catch (InterruptedException | IOException e) {
      Log.error("Could not add users for SASL/SCRAM authentication.");
      throw new RuntimeException(e);
    }
  }

  private void registerRootCA() {
    // Add root CA to container (obviates need for supplying it at CLI login '--ca-cert-path')
    runToolScript(
        "cp /etc/kafka/secrets/snakeoil-ca-1.crt /usr/local/share/ca-certificates && /usr/sbin/update-ca-certificates"
    );
  }

  private void runToolScript(String script) {
    try {
      tools.execInContainer("bash", "-c", script);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Do we have any of the CP Demo containers in running state?
   */
  private boolean isCpDemoRunningAnyContainer() {
    return getContainerStream().anyMatch(container -> container.getState().equals("running"));
  }

  /**
   * Do we have a CP Demo environment running with all containers?
   */
  private boolean isCpDemoRunningAllContainers() {
    // If empty, return false
    var containers = getContainerStream().collect(Collectors.toUnmodifiableSet());
    if (containers.isEmpty()) {
      return false;
    }

    return containers
        .stream()
        .allMatch(container -> container.getState().equals("running"));
  }

  private static Stream<Container> getContainerStream() {
    return DockerClientFactory
        .instance()
        .client()
        .listContainersCmd()
        .withShowAll(true)
        .exec()
        .stream()
        .filter(
            container -> CP_DEMO_CONTAINERS
                .stream()
                .anyMatch(c -> Arrays.asList(container.getNames()).contains(c))
        );
  }

  private void removeCPDemoContainersIfStopped() {
    CP_DEMO_CONTAINERS.forEach(container -> {
      try {
        DockerClientFactory
            .instance()
            .client()
            .listContainersCmd()
            .withShowAll(true)
            .exec()
            .stream()
            .filter(c -> c.getNames()[0].contains(container))
            .forEach(c -> {
              if (c.getState().equals("exited")) {
                DockerClientFactory
                    .instance()
                    .client()
                    .removeContainerCmd(c.getId())
                    .exec();
              }
            });
      } catch (Exception e) {
        Log.error("Error deleting stopped containers", e);
      }
    });
  }

  private static void shutdownContainers() {
    CP_DEMO_CONTAINERS.forEach(container -> {
      try {
        DockerClientFactory
            .instance()
            .client()
            .listContainersCmd()
            .withShowAll(true)
            .exec()
            .stream()
            .filter(c -> c.getNames()[0].contains(container))
            .forEach(c -> {
              DockerClientFactory
                  .instance()
                  .client()
                  .stopContainerCmd(c.getId())
                  .exec();
            });
      } catch (Exception e) {
        Log.error("Error deleting stopped containers", e);
      }
    });

    // Remove the network
    DockerClientFactory
        .instance()
        .client()
        .listNetworksCmd()
        .exec()
        .stream()
        .filter(network -> network.getName().equals("cp-demo"))
        .forEach(network -> {
          DockerClientFactory
              .instance()
              .client()
              .removeNetworkCmd(network.getId())
              .exec();
        });
  }

  @Override
  public Optional<ConnectionSpec> localConnectionSpec() {
    return Optional.empty();
  }

  @Override
  public Optional<ConnectionSpec> directConnectionSpec() {
    var cwd = System.getProperty("user.dir");
    var schemaRegistryTrustStoreLocation = new File(cwd,
        ".cp-demo/scripts/security/kafka.schemaregistry.truststore.jks"
    ).getAbsolutePath();
    var password = new Password("confluent".toCharArray());
    var kafkaTrustStoreLocation = new File(cwd,
        ".cp-demo/scripts/security/kafka.kafka1.truststore.jks"
    ).getAbsolutePath();

    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection",
            "Direct to Local",
            ConnectionSpecKafkaClusterConfigBuilder
                .builder()
                .bootstrapServers("localhost:11091")
                .tlsConfig(TLSConfigBuilder
                    .builder()
                    // TODO: Figure out what the keystore config needs to be
                    //       for mutual TLS.
                    .truststore(new TLSConfig.TrustStore(
                        kafkaTrustStoreLocation,
                        password,
                        null
                    ))
                    .enabled(true)
                    .build()
                )
                .build(),
            ConnectionSpecSchemaRegistryConfigBuilder
              .builder()
              .id("local-sr-cp-demo")
              .uri("https://localhost:8085")
              .credentials(
                  new BasicCredentials(
                      "superUser",
                      new Password("superUser".toCharArray())
                  )
              )
              .tlsConfig(new TLSConfig(
                  schemaRegistryTrustStoreLocation, password
              ))
              .build()
        )
    );
  }

  public Optional<ConnectionSpec> directConnectionSpecWithoutSR() {
    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection-no-sr",
            "Direct to Local (No SR)",
            ConnectionSpecKafkaClusterConfigBuilder
                .builder()
                .bootstrapServers("localhost:12091,localhost:12092")
                // Disable TLS
                .tlsConfig(TLSConfigBuilder.builder().enabled(false).build())
                .build(),
            null
        )
    );
  }

  public Optional<ConnectionSpec> directConnectionBasicAuth() {
    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection-basic-auth",
            "Direct to Local (Basic Auth)",
            ConnectionSpecKafkaClusterConfigBuilder
                .builder()
                .bootstrapServers("localhost:13091,localhost:13092")
                .credentials(new BasicCredentials(
                    "admin",
                    new Password("admin-secret".toCharArray())
                ))
                // Disable TLS
                .tlsConfig(TLSConfigBuilder.builder().enabled(false).build())
                .build(),
            null
        ));
  }

  public Optional<ConnectionSpec> directConnectionSaslScramAuth() {
    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-connection-sasl-scram-auth",
            "Direct connection (SASL/SCRAM Auth)",
            ConnectionSpecKafkaClusterConfigBuilder
                .builder()
                .bootstrapServers("localhost:15091")
                .credentials(
                    new ScramCredentials(
                        HashAlgorithm.SCRAM_SHA_256,
                        "admin",
                        new Password("admin-secret".toCharArray())
                    )
                )
                // Disable TLS
                .tlsConfig(TLSConfigBuilder.builder().enabled(false).build())
                .build(),
            null
        )
    );
  }

  /**
   * Taken from https://github.com/testcontainers/testcontainers-java/issues/3081#issuecomment-1553064952
   */
  public static Network createReusableNetwork(String name) {
    if (!TestcontainersConfiguration.getInstance().environmentSupportsReuse()) {
      return Network.newNetwork();
    }

    String id = DockerClientFactory
        .instance()
        .client()
        .listNetworksCmd()
        .exec()
        .stream()
        .filter(network ->
            network.getName().equals(name)
                && network.getLabels().equals(DockerClientFactory.DEFAULT_LABELS)
        )
        .map(com.github.dockerjava.api.model.Network::getId)
        .findFirst()
        .orElseGet(() -> DockerClientFactory
            .instance()
            .client()
            .createNetworkCmd()
            .withName(name)
            .withCheckDuplicate(true)
            .withLabels(DockerClientFactory.DEFAULT_LABELS)
            .exec()
            .getId()
        );

    return new Network() {
      @Override
      public Statement apply(Statement base, Description description) {
        return base;
      }

      @Override
      public String getId() {
        return id;
      }

      @Override
      public void close() {
        // never close
      }
    };
  }

  private void runScript(String path) {
    var pb = new ProcessBuilder(path);
    pb.inheritIO();
    try {
      var process = pb.start();
      process.waitFor();
      if (process.exitValue() != 0) {
        throw new RuntimeException("Script failed with exit code " + process.exitValue());
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Main method to start the test environment if used as a standalone application.
   */
  @SetEnvironmentVariable(key = "TESTCONTAINERS_RYUK_DISABLED", value = "true")
  public static void main(String[] args) {
    var env = new CPDemoTestEnvironment();
    if (args.length == 1 && args[0].equals("stop")) {
      Log.info("Stopping CP Demo environment...");
      env.shutdown();
      Log.info("CP Demo environment stopped.");
    } else {
      Log.info("Starting CP Demo environment...");
      env.start();
      Log.info("CP Demo environment started. Use make cp-demo-stop to stop it.");
    }
  }
}
