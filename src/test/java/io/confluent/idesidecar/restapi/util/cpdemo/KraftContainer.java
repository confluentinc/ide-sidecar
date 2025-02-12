package io.confluent.idesidecar.restapi.util.cpdemo;

import static io.confluent.idesidecar.restapi.util.cpdemo.Constants.DEFAULT_CONFLUENT_DOCKER_TAG;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.HealthCheck;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class KraftContainer extends GenericContainer<KraftContainer> {
  private static final Logger LOGGER = Logger.getLogger(KraftContainer.class.getName());
  private static final int KRAFT_PORT = 9092;
  private static final int KRAFT_CONTROLLER_PORT = 9093;
  private static final String DEFAULT_IMAGE = "confluentinc/cp-kafka";
  private static final String CONTAINER_NAME = "kraft";

  public KraftContainer(String tag, Network network) {
    super(DEFAULT_IMAGE + ":" + tag);
    LOGGER.info("Initializing KraftContainer with tag: " + tag);
    super.withNetwork(network);
    super.withNetworkAliases(CONTAINER_NAME);
    super.addFixedExposedPort(KRAFT_PORT, KRAFT_PORT);
    super.addFixedExposedPort(KRAFT_CONTROLLER_PORT, KRAFT_CONTROLLER_PORT);
    super
        .withEnv(getKraftEnv())
        .withCreateContainerCmdModifier(cmd -> cmd
            .withHealthcheck(new HealthCheck()
                .withTest(List.of("CMD", "bash", "-c", "echo srvr | nc kraft 9092 || exit 1"))
                .withInterval(TimeUnit.SECONDS.toNanos(2))
                .withRetries(25))
            .withName(CONTAINER_NAME)
            .withHostName(CONTAINER_NAME)
        );

    super.withFileSystemBind(
        ".cp-demo/scripts/security/",
        "/etc/kafka/secrets"
    );
    super.withReuse(true);
  }

  public KraftContainer(Network network) {
    this(DEFAULT_CONFLUENT_DOCKER_TAG, network);
  }

  public Map<String, String> getKraftEnv() {
    var envs = new HashMap<String, String>();
    envs.put("KAFKA_CFG_NODE_ID", "1");
    envs.put("KAFKA_CFG_PROCESS_ROLES", "broker,controller");
    envs.put("KAFKA_CFG_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093");
    envs.put("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
    envs.put("KAFKA_CFG_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093");
    envs.put("KAFKA_CFG_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
    envs.put("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
    envs.put("KAFKA_SSL_KEYSTORE_LOCATION", "/etc/kafka/secrets/kafka.kraft.keystore.jks");
    envs.put("KAFKA_SSL_KEYSTORE_PASSWORD", "confluent");
    envs.put("KAFKA_SSL_KEYSTORE_TYPE", "PKCS12");
    envs.put("KAFKA_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/kafka.kraft.truststore.jks");
    envs.put("KAFKA_SSL_TRUSTSTORE_PASSWORD", "confluent");
    envs.put("KAFKA_SSL_TRUSTSTORE_TYPE", "JKS");
    envs.put("KAFKA_SSL_CIPHER_SUITES", Constants.SSL_CIPHER_SUITES);
    envs.put("KAFKA_SSL_CLIENT_AUTH", "required");
    envs.put("KAFKA_OPTS", "-Djava.security.auth.login.config=/etc/kafka/secrets/kraft_jaas.conf");
    LOGGER.info("Kraft environment variables: " + envs);
    return envs;
  }

  @Override
  protected void containerIsStarting(InspectContainerResponse containerInfo) {
    super.containerIsStarting(containerInfo);
    LOGGER.info("KraftContainer is starting with info: " + containerInfo);
  }

  @Override
  protected void containerIsStarted(InspectContainerResponse containerInfo) {
    super.containerIsStarted(containerInfo);
    LOGGER.info("KraftContainer has started with info: " + containerInfo);
  }

  @Override
  protected void containerIsStopping(InspectContainerResponse containerInfo) {
    super.containerIsStopping(containerInfo);
    LOGGER.info("KraftContainer is stopping with info: " + containerInfo);
  }

}