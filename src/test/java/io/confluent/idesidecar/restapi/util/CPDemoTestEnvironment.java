package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.credentials.BasicCredentials;
import io.confluent.idesidecar.restapi.credentials.MutualTLSCredentials;
import io.confluent.idesidecar.restapi.credentials.Password;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.util.cpdemo.*;
import io.confluent.idesidecar.restapi.util.cpdemo.SchemaRegistryContainer;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link TestEnvironment} that starts a CP Demo environment with a two-node Kafka cluster,
 * Zookeeper, OpenLDAP, and Schema Registry.
 * Modeled after https://github.com/confluentinc/cp-demo/blob/7.7.1-post/docker-compose.yml
 */
@SetEnvironmentVariable(key = "TESTCONTAINERS_REUSE_ENABLE", value = "true")
public class CPDemoTestEnvironment implements TestEnvironment {

  private Network network;
  private ToolsContainer tools;
  private ZookeeperContainer zookeeper;
  private OpenldapContainer ldap;
  private CPServerContainer kafka1;
  private CPServerContainer kafka2;
  private SchemaRegistryContainer schemaRegistry;

  @Override
  public void start() {
    network = createReusableNetwork("cp-demo");
    // Check if zookeeper, kafka1, kafka2, ldap, schemaRegistry are already running
    var cpDemoRunning = DockerClientFactory
        .instance()
        .client()
        .listContainersCmd()
        .withShowAll(true)
        .exec()
        .stream()
        .anyMatch(container -> container.getNames()[0].contains("zookeeper")
            || container.getNames()[0].contains("kafka1")
            || container.getNames()[0].contains("kafka2")
            || container.getNames()[0].contains("openldap")
            || container.getNames()[0].contains("schemaregistry")
        );

    tools = new ToolsContainer(network);
    tools.start();
    // Add root CA to container (obviates need for supplying it at CLI login '--ca-cert-path')
    if (!cpDemoRunning) {
      try {
        tools.execInContainer(
            "bash",
            "-c",
            "cp /etc/kafka/secrets/snakeoil-ca-1.crt /usr/local/share/ca-certificates && /usr/sbin/update-ca-certificates"
        );
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    zookeeper = new ZookeeperContainer("7.5.1", network);
    zookeeper.waitingFor(Wait.forHealthcheck());
    zookeeper.start();

    ldap = new OpenldapContainer(network);
    ldap.start();

    kafka1 = new CPServerContainer(
        "7.5.1",
        network,
        "kafka1",
        8091,
        9091,
        10091,
        11091,
        12091
    );
    kafka1.withEnv(Map.of(
        "KAFKA_BROKER_ID", "1",
        "KAFKA_BROKER_RACK", "r1",
        "KAFKA_JMX_PORT", "9991"
    ));
    kafka2 = new CPServerContainer(
        "7.5.1",
        network,
        "kafka2",
        8092,
        9092,
        10092,
        11092,
        12092
    );
    kafka2.withEnv(Map.of(
        "KAFKA_BROKER_ID", "2",
        "KAFKA_BROKER_RACK", "r2",
        "KAFKA_JMX_PORT", "9992"
    ));

    // Must be started in parallel
    Startables.deepStart(List.of(kafka1, kafka2)).join();
    if (!cpDemoRunning) {
      try {
        tools.execInContainer(
            "bash",
            "-c",
            "/tmp/helper/create-role-bindings.sh"
        );
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      try {
        kafka1.execInContainer(
            "kafka-configs",
            "--bootstrap-server", "kafka1:12091",
            "--entity-type", "topics",
            "--entity-name", "_confluent-metadata-auth",
            "--alter",
            "--add-config", "min.insync.replicas=1"
        );
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    schemaRegistry = new SchemaRegistryContainer("7.5.1", network);
    schemaRegistry.start();
  }

  @Override
  public void shutdown() {

  }

  @Override
  public Optional<ConnectionSpec> localConnectionSpec() {
    return Optional.empty();
  }

  @Override
  public Optional<ConnectionSpec> directConnectionSpec() {
    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection",
            "Direct to Local",
            new ConnectionSpec.KafkaClusterConfig(
                "localhost:12091,localhost:12092",
                null,
                false,
                false
            ),
            new ConnectionSpec.SchemaRegistryConfig(
                null,
                "https://localhost:8085",
                new BasicCredentials(
                    "schemaregistryUser",
                    new Password("schemaregistryUser".toCharArray())
                ),
                false
            )
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
}
