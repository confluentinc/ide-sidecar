package io.confluent.idesidecar.restapi.util.cpdemo;

import static io.confluent.idesidecar.restapi.util.cpdemo.Constants.DEFAULT_CONFLUENT_DOCKER_TAG;

import com.github.dockerjava.api.model.HealthCheck;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

public class CPServerContainer extends GenericContainer<CPServerContainer> {

  private static final String DEFAULT_IMAGE = "confluentinc/cp-server";
  private static final String CLUSTER_ID = "sidecar-cp-integration-test-cluster";

  private final String tag;
  private final Integer nodeId;
  private final String containerName;
  private final Integer mdsPort;
  private final Integer internalPort;
  private final Integer tokenPort;
  private final Integer sslPort;
  private final Integer clearPort;
  private final Integer internalClearPort;
  private final Integer internalHostPort;
  private final Integer tokenHostPort;
  private final Integer scramPort;
  private final Integer controllerPort;

  public CPServerContainer(
      String tag,
      Network network,
      String containerName,
      Integer nodeId,
      Integer mdsPort,
      Integer internalPort,
      Integer tokenPort,
      Integer sslPort,
      Integer clearPort,
      Integer internalClearPort,
      Integer internalHostPort,
      Integer tokenHostPort,
      Integer scramPort,
      Integer controllerPort
  ) {
    super(DEFAULT_IMAGE + ":" + tag);
    this.tag = tag;
    this.containerName = containerName;
    this.nodeId = nodeId;
    this.mdsPort = mdsPort;
    this.internalPort = internalPort;
    this.tokenPort = tokenPort;
    this.sslPort = sslPort;
    this.clearPort = clearPort;
    this.internalClearPort = internalClearPort;
    this.internalHostPort = internalHostPort;
    this.tokenHostPort = tokenHostPort;
    this.scramPort = scramPort;
    this.controllerPort = controllerPort;

    super.withNetwork(network);
    super.withNetworkAliases(containerName);
    super
        .withEnv(kraftEnv())
        .withEnv(listenersEnv(
            internalPort, tokenPort, sslPort, clearPort, internalClearPort, internalHostPort,
            tokenHostPort, scramPort, controllerPort
        ))
        .withEnv(sslEnv())
        .withEnv(confluentSchemaValidationEnv())
        .withEnv(mdsEnv(mdsPort))
        .withEnv(ldapEnv())
        .withEnv(embeddedKafkaRest())
        .withEnv(otherEnvs())
        .withCreateContainerCmdModifier(cmd -> cmd
            .withAliases(containerName)
            .withName(containerName)
            .withHostName(containerName)
            .withHealthcheck(new HealthCheck()
                .withTest(List.of(
                    "CMD", "bash", "-c",
                    "curl --user superUser:superUser -fail --silent --insecure https://%s:%d/kafka/v3/clusters/ --output /dev/null || exit 1"
                        .formatted(containerName, mdsPort)))
                .withInterval(TimeUnit.SECONDS.toNanos(2))
                .withRetries(25)
            )
        );

    super.addFixedExposedPort(mdsPort, mdsPort);
    super.addFixedExposedPort(internalPort, internalPort);
    super.addFixedExposedPort(tokenPort, tokenPort);
    super.addFixedExposedPort(sslPort, sslPort);
    super.addFixedExposedPort(clearPort, clearPort);
    super.addFixedExposedPort(internalHostPort, internalHostPort);
    super.addFixedExposedPort(tokenHostPort, tokenHostPort);
    super.addFixedExposedPort(scramPort, scramPort);

    // This just sets the Waiting strategy, doesn't actually wait. I know, it's confusing.
    super.waitingFor(Wait.forHealthcheck());

    super.withFileSystemBind(
        ".cp-demo/scripts/security/keypair",
        "/tmp/conf"
    );
    super.withFileSystemBind(
        ".cp-demo/scripts/helper",
        "/tmp/helper"
    );
    super.withFileSystemBind(
        ".cp-demo/scripts/security",
        "/etc/kafka/secrets"
    );
    super.withReuse(true);
  }

  /**
   * Create a new cp-server container with the default tag
   *
   * @param network           The network to attach the container to
   * @param containerName     The name of the container
   * @param mdsPort           The port for the MDS Server
   * @param internalPort      The port for the INTERNAL listener
   * @param tokenPort         The port for the TOKEN listener
   * @param sslPort           The port for the SSL listener
   * @param clearPort         The port for the CLEAR listener on localhost
   * @param internalClearPort The port for the CLEAR listener
   * @param internalHostPort  The port for the INTERNAL listener on localhost
   * @param tokenHostPort     The port for the TOKEN listener on localhost
   * @param scramPort         The port for the SCRAM listener on localhost
   */
  public CPServerContainer(
      Network network,
      String containerName,
      Integer nodeId,
      Integer mdsPort,
      Integer internalPort,
      Integer tokenPort,
      Integer sslPort,
      Integer clearPort,
      Integer internalClearPort,
      Integer internalHostPort,
      Integer tokenHostPort,
      Integer scramPort,
      Integer controllerPort
  ) {
    this(
        DEFAULT_CONFLUENT_DOCKER_TAG,
        network,
        containerName,
        nodeId,
        mdsPort,
        internalPort,
        tokenPort,
        sslPort,
        clearPort,
        internalClearPort,
        internalHostPort,
        tokenHostPort,
        scramPort,
        controllerPort
    );
  }

  public Map<String, String> kraftEnv() {
    var env = new HashMap<String, String>();
    // This is known as "combined" mode. In combined mode, no two nodes can have
    // the same ID, but a combined broker/controller node counts as a single node.
    env.put("KAFKA_PROCESS_ROLES", "broker,controller");
    env.put("KAFKA_NODE_ID", nodeId.toString());
    env.put("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
    env.put("CLUSTER_ID", CLUSTER_ID);
    return env;
  }

  public Map<String, String> listenersEnv(
      Integer internalPort,
      Integer tokenPort,
      Integer sslPort,
      Integer clearPort,
      Integer internalClearPort,
      Integer internalHostPort,
      Integer tokenHostPort,
      Integer scramPort,
      Integer controllerPort
  ) {
    var env = new HashMap<String, String>();
    env.put(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "INTERNAL:SASL_PLAINTEXT,INTERNALHOST:SASL_PLAINTEXT,TOKEN:SASL_SSL,TOKENHOST:SASL_SSL,"
            + "SSL:SSL,CLEAR:PLAINTEXT,INTERNALCLEAR:PLAINTEXT,SCRAMHOST:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT"
    );
    env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL");
    env.put("KAFKA_LISTENERS",
        "INTERNAL://%s:%d,TOKEN://%s:%d,SSL://%s:%d,CLEAR://%s:%d,INTERNALCLEAR://%s:%d,INTERNALHOST://%s:%d,TOKENHOST://%s:%d,SCRAMHOST://%s:%d,CONTROLLER://%s:%d".formatted(
            containerName, internalPort,
            containerName, tokenPort,
            containerName, sslPort,
            containerName, clearPort,
            containerName, internalClearPort,
            containerName, internalHostPort,
            containerName, tokenHostPort,
            containerName, scramPort,
            containerName, controllerPort
        ));
    env.put("KAFKA_ADVERTISED_LISTENERS",
        "INTERNAL://%s:%d,TOKEN://%s:%d,SSL://%s:%d,CLEAR://%s:%d,INTERNALCLEAR://%s:%d,INTERNALHOST://%s:%d,TOKENHOST://%s:%d,SCRAMHOST://%s:%d".formatted(
            containerName, internalPort,
            containerName, tokenPort,
            "localhost", sslPort,
            "localhost", clearPort,
            containerName, internalClearPort,
            "localhost", internalHostPort,
            "localhost", tokenHostPort,
            "localhost", scramPort
        ));
    env.put("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN");
    env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN, OAUTHBEARER, SCRAM-SHA-256");

    env.put("KAFKA_LISTENER_NAME_INTERNAL_SASL_ENABLED_MECHANISMS", "PLAIN");
    env.put("KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG", """
        org.apache.kafka.common.security.plain.PlainLoginModule required \\
        username="admin" \\
        password="admin-secret" \\
        user_admin="admin-secret" \\
        user_mds="mds-secret";
        """);

    env.put("KAFKA_LISTENER_NAME_INTERNALHOST_SASL_ENABLED_MECHANISMS", "PLAIN");
    env.put("KAFKA_LISTENER_NAME_INTERNALHOST_PLAIN_SASL_JAAS_CONFIG", """
        org.apache.kafka.common.security.plain.PlainLoginModule required \\
        username="admin" \\
        password="admin-secret" \\
        user_admin="admin-secret" \\
        user_mds="mds-secret";
        """);

    // Configure TOKEN listener for Confluent Platform components and impersonation
    env.put("KAFKA_LISTENER_NAME_TOKEN_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS",
        "io.confluent.kafka.server.plugins.auth.token.TokenBearerValidatorCallbackHandler");
    env.put("KAFKA_LISTENER_NAME_TOKEN_OAUTHBEARER_SASL_LOGIN_CALLBACK_HANDLER_CLASS",
        "io.confluent.kafka.server.plugins.auth.token.TokenBearerServerLoginCallbackHandler");
    env.put("KAFKA_LISTENER_NAME_TOKEN_SASL_ENABLED_MECHANISMS", "OAUTHBEARER");
    env.put("KAFKA_LISTENER_NAME_TOKEN_OAUTHBEARER_SASL_JAAS_CONFIG", """
        org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \\
        publicKeyPath="/tmp/conf/public.pem";
        """);

    // Configure TOKENHOST listener
    env.put("KAFKA_LISTENER_NAME_TOKENHOST_OAUTHBEARER_SASL_SERVER_CALLBACK_HANDLER_CLASS",
        "io.confluent.kafka.server.plugins.auth.token.TokenBearerValidatorCallbackHandler");
    env.put("KAFKA_LISTENER_NAME_TOKENHOST_OAUTHBEARER_SASL_LOGIN_CALLBACK_HANDLER_CLASS",
        "io.confluent.kafka.server.plugins.auth.token.TokenBearerServerLoginCallbackHandler");
    env.put("KAFKA_LISTENER_NAME_TOKENHOST_SASL_ENABLED_MECHANISMS", "OAUTHBEARER");
    env.put("KAFKA_LISTENER_NAME_TOKENHOST_OAUTHBEARER_SASL_JAAS_CONFIG", """
        org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \\
        publicKeyPath="/tmp/conf/public.pem";
        """);

    // Configure SCRAMHOST listener
    env.put("KAFKA_LISTENER_NAME_SCRAMHOST_SCRAM-SHA-256_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-256");
    env.put("KAFKA_LISTENER_NAME_SCRAMHOST_SCRAM-SHA-256_SASL_JAAS_CONFIG", """
        org.apache.kafka.common.security.scram.ScramLoginModule required \\
        username="admin" \\
        password="admin-secret" \\
        user_admin="admin-secret";
        """);

    env.put("KAFKA_LISTENER_NAME_SSL_SSL_PRINCIPAL_MAPPING_RULES",
        "RULE:^CN=([a-zA-Z0-9.]*).*$$/$$1/ , DEFAULT");
    env.put("KAFKA_LISTENER_NAME_TOKEN_SSL_PRINCIPAL_MAPPING_RULES",
        "RULE:^CN=([a-zA-Z0-9.]*).*$$/$$1/ , DEFAULT");
    return env;
  }

  public Map<String, String> sslEnv() {
    var env = new HashMap<String, String>();
    env.put("KAFKA_SSL_KEYSTORE_FILENAME", "kafka.%s.keystore.jks".formatted(containerName));
    env.put("KAFKA_SSL_KEYSTORE_CREDENTIALS", "%s_keystore_creds".formatted(containerName));
    env.put("KAFKA_SSL_KEY_CREDENTIALS", "%s_sslkey_creds".formatted(containerName));
    env.put("KAFKA_SSL_TRUSTSTORE_FILENAME", "kafka.%s.truststore.jks".formatted(containerName));
    env.put("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", "%s_truststore_creds".formatted(containerName));
    env.put("KAFKA_SSL_CIPHER_SUITES", Constants.SSL_CIPHER_SUITES);
    env.put("KAFKA_SSL_CLIENT_AUTH", "requested");
    return env;
  }

  public Map<String, String> confluentSchemaValidationEnv() {
    var env = new HashMap<String, String>();
    env.put("KAFKA_CONFLUENT_SCHEMA_REGISTRY_URL", "https://schemaregistry:8085");
    env.put("KAFKA_CONFLUENT_BASIC_AUTH_CREDENTIALS_SOURCE", "USER_INFO");
    env.put("KAFKA_CONFLUENT_BASIC_AUTH_USER_INFO", "superUser:superUser");
    env.put("KAFKA_CONFLUENT_SSL_TRUSTSTORE_LOCATION",
        "/etc/kafka/secrets/kafka.%s.truststore.jks".formatted(this.containerName));
    env.put("KAFKA_CONFLUENT_SSL_TRUSTSTORE_PASSWORD", "confluent");
    return env;
  }

  public Map<String, String> mdsEnv(Integer mdsPort) {
    var env = new HashMap<String, String>();
    env.put("KAFKA_CONFLUENT_METADATA_TOPIC_REPLICATION_FACTOR", "1");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_AUTHENTICATION_METHOD", "BEARER");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_LISTENERS", "https://0.0.0.0:%d".formatted(mdsPort));
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_ADVERTISED_LISTENERS",
        "https://%s:%d".formatted(containerName, mdsPort));
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_SSL_TRUSTSTORE_LOCATION",
        "/etc/kafka/secrets/kafka.mds.truststore.jks");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_SSL_TRUSTSTORE_PASSWORD", "confluent");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_SSL_KEYSTORE_LOCATION",
        "/etc/kafka/secrets/kafka.mds.keystore.jks");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_SSL_KEYSTORE_PASSWORD", "confluent");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_SSL_KEY_PASSWORD", "confluent");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_SSL_CIPHER_SUITES", Constants.SSL_CIPHER_SUITES);
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_TOKEN_MAX_LIFETIME_MS", "3600000");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_TOKEN_SIGNATURE_ALGORITHM", "RS256");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_TOKEN_KEY_PATH", "/tmp/conf/keypair.pem");
    env.put("KAFKA_CONFLUENT_METADATA_SERVER_KRAFT_CONTROLLER_ENABLED", "true");
    return env;
  }

  public Map<String, String> ldapEnv() {
    var env = new HashMap<String, String>();
    env.put("KAFKA_LDAP_JAVA_NAMING_FACTORY_INITIAL", "com.sun.jndi.ldap.LdapCtxFactory");
    env.put("KAFKA_LDAP_COM_SUN_JNDI_LDAP_READ_TIMEOUT", "3000");
    env.put("KAFKA_LDAP_JAVA_NAMING_PROVIDER_URL", "ldap://openldap:389");
    env.put("KAFKA_LDAP_JAVA_NAMING_SECURITY_PRINCIPAL", "cn=admin,dc=confluentdemo,dc=io");
    env.put("KAFKA_LDAP_JAVA_NAMING_SECURITY_CREDENTIALS", "admin");
    env.put("KAFKA_LDAP_JAVA_NAMING_SECURITY_AUTHENTICATION", "simple");
    env.put("KAFKA_LDAP_SEARCH_MODE", "GROUPS");
    env.put("KAFKA_LDAP_GROUP_SEARCH_BASE", "ou=groups,dc=confluentdemo,dc=io");
    env.put("KAFKA_LDAP_GROUP_NAME_ATTRIBUTE", "cn");
    env.put("KAFKA_LDAP_GROUP_MEMBER_ATTRIBUTE", "memberUid");
    env.put("KAFKA_LDAP_GROUP_OBJECT_CLASS", "posixGroup");
    env.put("KAFKA_LDAP_GROUP_MEMBER_ATTRIBUTE_PATTERN", "cn=(.*),ou=users,dc=confluentdemo,dc=io");
    env.put("KAFKA_LDAP_USER_SEARCH_BASE", "ou=users,dc=confluentdemo,dc=io");
    env.put("KAFKA_LDAP_USER_NAME_ATTRIBUTE", "uid");
    env.put("KAFKA_LDAP_USER_OBJECT_CLASS", "inetOrgPerson");
    return env;
  }

  public Map<String, String> embeddedKafkaRest() {
    var env = new HashMap<String, String>();
    // Hardcoded values
    env.put("KAFKA_KAFKA_REST_BOOTSTRAP_SERVERS",
        "SASL_SSL://kafka1:10091,SASL_SSL://kafka2:10092");
    env.put("KAFKA_KAFKA_REST_CLIENT_SECURITY_PROTOCOL", "SASL_SSL");
    env.put("KAFKA_KAFKA_REST_CLIENT_SSL_TRUSTSTORE_LOCATION",
        "/etc/kafka/secrets/kafka.%s.truststore.jks".formatted(containerName));
    env.put("KAFKA_KAFKA_REST_CLIENT_SSL_TRUSTSTORE_PASSWORD", "confluent");
    env.put("KAFKA_KAFKA_REST_CLIENT_SSL_KEYSTORE_LOCATION",
        "/etc/kafka/secrets/kafka.%s.keystore.jks".formatted(containerName));
    env.put("KAFKA_KAFKA_REST_CLIENT_SSL_KEYSTORE_PASSWORD", "confluent");
    env.put("KAFKA_KAFKA_REST_CLIENT_SSL_KEY_PASSWORD", "confluent");
    env.put("KAFKA_KAFKA_REST_KAFKA_REST_RESOURCE_EXTENSION_CLASS",
        "io.confluent.kafkarest.security.KafkaRestSecurityResourceExtension");
    env.put("KAFKA_KAFKA_REST_REST_SERVLET_INITIALIZOR_CLASSES",
        "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler");
    env.put("KAFKA_KAFKA_REST_PUBLIC_KEY_PATH", "/tmp/conf/public.pem");
    // Hardcoded values
    env.put("KAFKA_KAFKA_REST_CONFLUENT_METADATA_BOOTSTRAP_SERVER_URLS",
        "https://kafka1:8091,https://kafka2:8092");
    env.put("KAFKA_KAFKA_REST_SSL_TRUSTSTORE_LOCATION",
        "/etc/kafka/secrets/kafka.%s.truststore.jks".formatted(containerName));
    env.put("KAFKA_KAFKA_REST_SSL_TRUSTSTORE_PASSWORD", "confluent");
    env.put("KAFKA_KAFKA_REST_CONFLUENT_METADATA_HTTP_AUTH_CREDENTIALS_PROVIDER", "BASIC");
    env.put("KAFKA_KAFKA_REST_CONFLUENT_METADATA_BASIC_AUTH_USER_INFO", "restAdmin:restAdmin");
    env.put("KAFKA_KAFKA_REST_CONFLUENT_METADATA_SERVER_URLS_MAX_AGE_MS", "60000");
    env.put("KAFKA_KAFKA_REST_CLIENT_CONFLUENT_METADATA_SERVER_URLS_MAX_AGE_MS", "60000");
    return env;
  }

  public Map<String, String> otherEnvs() {
    var env = new HashMap<String, String>();
    env.put("KAFKA_OPTS", "-Djava.security.auth.login.config=/etc/kafka/secrets/broker_jaas.conf");
    env.put("KAFKA_AUTHORIZER_CLASS_NAME",
        "io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer");
    env.put("KAFKA_CONFLUENT_AUTHORIZER_ACCESS_RULE_PROVIDERS", "CONFLUENT,KRAFT_ACL");
    env.put("KAFKA_SUPER_USERS", "User:admin;User:mds;User:superUser;User:ANONYMOUS");
    env.put("KAFKA_LOG4J_LOGGERS", "kafka.authorizer.logger=INFO");
    env.put("KAFKA_LOG4J_ROOT_LOGLEVEL", "INFO");

    env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
    env.put("KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR", "1");
    env.put("KAFKA_CONFLUENT_SECURITY_EVENT_LOGGER_EXPORTER_KAFKA_TOPIC_REPLICAS", "2");
    env.put("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
    env.put("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
    env.put("KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR", "1");
    env.put("KAFKA_CONFLUENT_BALANCER_HEAL_BROKER_FAILURE_THRESHOLD_MS", "30000");
    env.put("KAFKA_DELETE_TOPIC_ENABLE", "true");
    env.put("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    env.put("KAFKA_DEFAULT_REPLICATION_FACTOR", "1");

    return env;
  }

  public String getTag() {
    return tag;
  }

  public String getContainerName() {
    return containerName;
  }

  public String getClusterId() {
    return CLUSTER_ID;
  }

  public Integer getMdsPort() {
    return mdsPort;
  }

  public Integer getInternalPort() {
    return internalPort;
  }

  public Integer getTokenPort() {
    return tokenPort;
  }

  public Integer getSslPort() {
    return sslPort;
  }

  public Integer getClearPort() {
    return clearPort;
  }

  public Integer getScramPort() {
    return scramPort;
  }
}
