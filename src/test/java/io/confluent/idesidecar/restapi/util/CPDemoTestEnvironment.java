package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.util.cpdemo.CPServerContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.OpenldapContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.SchemaRegistryContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.ZookeeperContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

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
    network = Network.newNetwork();
    zookeeper = new ZookeeperContainer(network);
    zookeeper.start();
    zookeeper.waitingFor(Wait.forHealthcheck());

    ldap = new OpenldapContainer(network);
    ldap.start();

    kafka1 = new CPServerContainer(
        network,
        "kafka1",
        8091,
        9091,
        10091,
        11091,
        12091
    );
    kafka2 = new CPServerContainer(
        network,
        "kafka2",
        8092,
        9092,
        10092,
        11092,
        12092
    );

    kafka1.start();
    kafka2.start();
    kafka1.waitingFor(Wait.forHealthcheck());
    kafka2.waitingFor(Wait.forHealthcheck());

    schemaRegistry = new SchemaRegistryContainer(network);
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
