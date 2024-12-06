package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.util.cpdemo.CPServerContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.OpenldapContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.SchemaRegistryContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.ZookeeperContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.Map;
import java.util.Optional;

/**
 * A {@link TestEnvironment} that starts a CP Demo environment with a two-node Kafka cluster,
 * Zookeeper, OpenLDAP, and Schema Registry.
 * Modeled after https://github.com/confluentinc/cp-demo/blob/7.7.1-post/docker-compose.yml
 */
public class CPDemoTestEnvironment implements TestEnvironment {

  private Network network;
  private ZookeeperContainer zookeeper;
  private OpenldapContainer ldap;
  private CPServerContainer kafka1;
  private CPServerContainer kafka2;
  private SchemaRegistryContainer schemaRegistry;

  @Override
  public void start() {
    // Run .cp-demo/scripts/start_cp.sh
    ProcessBuilder pb = new ProcessBuilder(
        ".cp-demo/scripts/start_cp.sh"
    );
    pb.environment().put("CONFLUENT_DOCKER_TAG", "7.5.1");
    pb.inheritIO();
    try {
      Process p = pb.start();
      p.waitFor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    network = Network.newNetwork();
    zookeeper = new ZookeeperContainer("7.5.1", network);
    zookeeper.start();
    zookeeper.waitingFor(Wait.forHealthcheck());

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

    kafka1.start();
    kafka2.start();
    kafka1.waitingFor(Wait.forHealthcheck());
    kafka2.waitingFor(Wait.forHealthcheck());

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
    schemaRegistry.waitingFor(Wait.forHealthcheck());
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
            new ConnectionSpec.SchemaRegistryConfig(
                null,
                "http://localhost:%d".formatted(schemaRegistry.getPort()),
                null
            )
        )
    );
  }
}
