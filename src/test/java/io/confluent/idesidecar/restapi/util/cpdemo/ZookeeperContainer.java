package io.confluent.idesidecar.restapi.util.cpdemo;

import com.github.dockerjava.api.model.HealthCheck;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {
  private static final int ZOOKEEPER_PORT = 2181;
  private static final int ZOOKEEPER_SECURE_PORT = 2182;
  private static final String DEFAULT_IMAGE = "confluentinc/cp-zookeeper";
  private static final String DEFAULT_TAG = "7.7.1";
  private static final String CONTAINER_NAME = "zookeeper";

  public ZookeeperContainer(String tag, Network network) {
    super(DEFAULT_IMAGE + ":" + tag);
    super.withNetwork(network);
    super.withNetworkAliases(CONTAINER_NAME);
    super.addFixedExposedPort(ZOOKEEPER_PORT, ZOOKEEPER_PORT);
    super.addFixedExposedPort(ZOOKEEPER_SECURE_PORT, ZOOKEEPER_SECURE_PORT);
    super
        .withEnv(getZookeeperEnv())
        .withCreateContainerCmdModifier(cmd -> cmd
            .withHealthcheck(new HealthCheck()
                .withTest(List.of("CMD", "bash", "-c", "echo srvr | nc zookeeper 2181 || exit 1"))
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

  public ZookeeperContainer(Network network) {
    this(DEFAULT_TAG, network);
  }

  public Map<String, String> getZookeeperEnv() {
      var envs = new HashMap<String, String>();
      envs.put("ZOOKEEPER_CLIENT_PORT", "2181");
      envs.put("ZOOKEEPER_TICK_TIME", "2000");
      envs.put("ZOOKEEPER_SECURE_CLIENT_PORT", "2182");
      envs.put("ZOOKEEPER_SERVER_CNXN_FACTORY", "org.apache.zookeeper.server.NettyServerCnxnFactory");
      envs.put("ZOOKEEPER_SSL_KEYSTORE_LOCATION", "/etc/kafka/secrets/kafka.zookeeper.keystore.jks");
      envs.put("ZOOKEEPER_SSL_KEYSTORE_PASSWORD", "confluent");
      envs.put("ZOOKEEPER_SSL_KEYSTORE_TYPE", "PKCS12");
      envs.put("ZOOKEEPER_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/kafka.zookeeper.truststore.jks");
      envs.put("ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD", "confluent");
      envs.put("ZOOKEEPER_SSL_TRUSTSTORE_TYPE", "JKS");
      envs.put("ZOOKEEPER_SSL_CIPHER_SUITES", Constants.SSL_CIPHER_SUITES);
      envs.put("ZOOKEEPER_SSL_CLIENT_AUTH", "need");
      envs.put("ZOOKEEPER_AUTH_PROVIDER_X509", "org.apache.zookeeper.server.auth.X509AuthenticationProvider");
      envs.put("ZOOKEEPER_AUTH_PROVIDER_SASL", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
      envs.put("KAFKA_OPTS", "-Djava.security.auth.login.config=/etc/kafka/secrets/zookeeper_jaas.conf");
      return envs;
    }
}
