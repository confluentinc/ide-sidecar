package io.confluent.idesidecar.restapi.util;

import com.github.dockerjava.api.model.HealthCheck;
import io.confluent.idesidecar.restapi.credentials.Credentials;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.util.cpdemo.*;
import io.confluent.idesidecar.restapi.util.cpdemo.SchemaRegistryContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.lifecycle.Startables;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

  @Override
  public void start() {
    // Run .cp-demo/scripts/start_cp.sh
    runScript(".cp-demo/scripts/start_cp.sh");

    network = Network.newNetwork();

    tools = new ToolsContainer(network);
    tools.waitingFor(Wait.forHealthcheck());
    tools.start();
    // Add root CA to container (obviates need for supplying it at CLI login '--ca-cert-path')
    try {
      tools.execInContainer(
          "bash",
          "-c",
          "cp /etc/kafka/secrets/snakeoil-ca-1.crt /usr/local/share/ca-certificates && /usr/sbin/update-ca-certificates"
      );
    } catch (Exception e) {
      throw new RuntimeException(e);
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
    schemaRegistry = new SchemaRegistryContainer("7.5.1", network);

    schemaRegistry.start();
  }

  private static void runScript(String script) {
    ProcessBuilder pb = new ProcessBuilder(script);
    // pb.directory(new File(".cp-demo/scripts"));
    pb.environment().put("CONFLUENT_DOCKER_TAG", "7.5.1");
    pb.environment().put("REPOSITORY", "confluentinc");
    pb.inheritIO();
    try {
      Process p = pb.start();
      p.waitFor();
      if (p.exitValue() != 0) {
        throw new RuntimeException("Failed to run %s".formatted(script));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    if (zookeeper != null) {
      zookeeper.stop();
    }
    if (ldap != null) {
      ldap.stop();
    }
    if (kafka1 != null) {
      kafka1.stop();
    }
    if (kafka2 != null) {
      kafka2.stop();
    }
    if (schemaRegistry != null) {
      schemaRegistry.stop();
    }
    if (tools != null) {
      tools.stop();
    }
    if (network != null) {
      network.close();
    }
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
            null
        )
    );
  }
}
