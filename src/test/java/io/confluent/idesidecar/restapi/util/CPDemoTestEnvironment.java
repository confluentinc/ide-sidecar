package io.confluent.idesidecar.restapi.util;

import static io.confluent.idesidecar.restapi.util.cpdemo.Constants.BROKER_JAAS_CONTENTS;

import io.confluent.idesidecar.restapi.credentials.BasicCredentials;
import io.confluent.idesidecar.restapi.credentials.Password;
import io.confluent.idesidecar.restapi.credentials.TLSConfig;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.util.cpdemo.CPServerContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.OpenldapContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.SchemaRegistryContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.ToolsContainer;
import io.confluent.idesidecar.restapi.util.cpdemo.ZookeeperContainer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.SslFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;
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

    // Write BROKER_JAAS_CONTENTS to .cp-demo/scripts/security/broker_jaas.conf
    try {
      Files.write(
          new File(".cp-demo/scripts/security/broker_jaas.conf").toPath(),
          BROKER_JAAS_CONTENTS.getBytes()
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    kafka1 = new CPServerContainer(
        "7.5.1",
        network,
        "kafka1",
        8091,
        9091,
        10091,
        11091,
        12091,
        13091,
        14091
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
        12092,
        13092,
        14092
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
    var cwd = System.getProperty("user.dir");
    var trustStoreLocation = new File(cwd,
        ".cp-demo/scripts/security/kafka.schemaregistry.truststore.jks"
    ).getAbsolutePath();
    var trustStorePassword = new Password("confluent".toCharArray());
    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection",
            "Direct to Local",
            new ConnectionSpec.KafkaClusterConfig(
                // Use CLEAR listener
                "localhost:12091,localhost:12092",
                null
            ),
            new ConnectionSpec.SchemaRegistryConfig(
                "something",
                "https://localhost:8085",
                new BasicCredentials(
                    "superUser",
                    new Password("superUser".toCharArray())
                )
            ),
            new TLSConfig(trustStoreLocation, trustStorePassword)
        )
    );
  }

  public Optional<ConnectionSpec> directConnectionSpecWithoutSR() {
    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection-no-sr",
            "Direct to Local (No SR)",
            new ConnectionSpec.KafkaClusterConfig(
                "localhost:12091,localhost:12092",
                null
            ),
            null
        )
    );
  }

  public Optional<ConnectionSpec> directConnectionBasicAuth() {
    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection-basic-auth",
            "Direct to Local (Basic Auth)",
            new ConnectionSpec.KafkaClusterConfig(
                "localhost:13091,localhost:13092",
                new BasicCredentials(
                    "admin",
                    new Password("admin-secret".toCharArray())
                )
            ),
            null
        )
    );
  }

  public Optional<ConnectionSpec> directConnectionOverMutualTLS() {
    var cwd = System.getProperty("user.dir");
    var trustStoreLocation = new File(cwd,
        ".cp-demo/scripts/security/kafka.schemaregistry.truststore.jks"
    ).getAbsolutePath();
    var trustStorePassword = new Password("confluent".toCharArray());

    return Optional.of(
        ConnectionSpec.createDirect(
            "direct-to-local-connection-mtls",
            "Direct to Local (Mutual TLS)",
            new ConnectionSpec.KafkaClusterConfig(
                // Use SSL listener
                "localhost:11091,localhost:11092",
                null
            ),
            new ConnectionSpec.SchemaRegistryConfig(
                "something",
                "https://localhost:8085",
                new BasicCredentials(
                    "superUser",
                    new Password("superUser".toCharArray())
                )
            ),
            new TLSConfig(trustStoreLocation, trustStorePassword)
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

  @Test
  public void testSRDirectly()
      throws RestClientException, IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
    var cwd = System.getProperty("user.dir");
    var trustStorePath = new File(cwd,
        ".cp-demo/scripts/security/kafka.schemaregistry.truststore.jks").getAbsolutePath();
    var restService = new RestService("https://localhost:8085");
    var configs = Map.of(
        "schema.registry.url", "https://localhost:8085",
        "ssl.endpoint.identification.algorithm", "",
        "ssl.truststore.location", trustStorePath,
        "ssl.truststore.password", "confluent",
        "basic.auth.credentials.source", "USER_INFO",
        "basic.auth.user.info", "superUser:superUser"
    );

    restService.configure(configs);
    SslFactory sslFactory = new SslFactory(configs);
    if (sslFactory.sslContext() != null) {
      restService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
    }

    var client = new CachedSchemaRegistryClient(restService, 1000);
    var subjects = client.getAllSubjects();
    System.out.println(subjects);
  }
}
