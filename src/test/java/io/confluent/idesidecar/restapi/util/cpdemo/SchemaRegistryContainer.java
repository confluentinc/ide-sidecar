package io.confluent.idesidecar.restapi.util.cpdemo;

import com.github.dockerjava.api.model.HealthCheck;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
  private static final Integer PORT = 8085;
  private static final String DEFAULT_IMAGE = "confluentinc/cp-schema-registry";
  private static final String DEFAULT_TAG = "7.7.1";
  private static final String CONTAINER_NAME = "schemaregistry";

  public SchemaRegistryContainer(String tag, Network network) {
    super(DEFAULT_IMAGE + ":" + tag);
    super.withNetwork(network);
    super.withNetworkAliases(CONTAINER_NAME);
    super.addFixedExposedPort(PORT, PORT);
    super.withEnv(getSchemaRegistryEnv());

    super.withCreateContainerCmdModifier(cmd -> cmd.withHealthcheck(new HealthCheck()
            .withTest(List.of(
                "CMD",
                "bash",
                "-c",
                ("curl --user superUser:superUser --fail --silent " +
                    "--insecure https://schemaregistry:%d/subjects --output /dev/null " +
                    "|| exit 1").formatted(PORT))
            )
            .withInterval(TimeUnit.SECONDS.toNanos(2))
            .withRetries(25)
        )
        .withName(CONTAINER_NAME)
        .withHostName(CONTAINER_NAME)
    );
    super.waitingFor(Wait.forHealthcheck());
    super.withFileSystemBind(
        ".cp-demo/scripts/security",
        "/etc/kafka/secrets"
    );
    super.withFileSystemBind(
        ".cp-demo/scripts/security/keypair",
        "/tmp/conf"
    );
  }

  public SchemaRegistryContainer(Network network) {
    this(DEFAULT_TAG, network);
  }

  public Map<String, String> getSchemaRegistryEnv() {
    var envs = new HashMap<String, String>();
    envs.put("SCHEMA_REGISTRY_HOST_NAME", "schemaregistry");
    // Hardcoded values
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka1:10091,kafka2:10092");
    envs.put("SCHEMA_REGISTRY_LISTENERS", "https://0.0.0.0:%d".formatted(PORT));
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL", "SASL_SSL");
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM", "OAUTHBEARER");
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_SASL_LOGIN_CALLBACK_HANDLER_CLASS", "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler");
    // Hardcoded values
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG", """
        org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \\
        username="schemaregistryUser" \\
        password="schemaregistryUser" \\
        metadataServerUrls="https://kafka1:8091,https://kafka2:8092";
        """);
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/kafka.schemaregistry.truststore.jks");
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_PASSWORD", "confluent");
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_LOCATION", "/etc/kafka/secrets/kafka.schemaregistry.keystore.jks");
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_SSL_KEYSTORE_PASSWORD", "confluent");
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_SSL_KEY_PASSWORD", "confluent");
    envs.put("SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/kafka.schemaregistry.truststore.jks");
    envs.put("SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD", "confluent");
    envs.put("SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION", "/etc/kafka/secrets/kafka.schemaregistry.keystore.jks");
    envs.put("SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD", "confluent");
    envs.put("SCHEMA_REGISTRY_SSL_KEY_PASSWORD", "confluent");
    envs.put("SCHEMA_REGISTRY_SSL_CLIENT_AUTHENTICATION", "NONE");
    envs.put("SCHEMA_REGISTRY_SCHEMA_REGISTRY_INTER_INSTANCE_PROTOCOL", "https");
    envs.put("SCHEMA_REGISTRY_LOG4J_ROOT_LOGLEVEL", "DEBUG");
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", "_schemas");
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "2");
    envs.put("SCHEMA_REGISTRY_SSL_CIPHER_SUITES", Constants.SSL_CIPHER_SUITES);
    envs.put("SCHEMA_REGISTRY_DEBUG", "true");
    envs.put("SCHEMA_REGISTRY_SCHEMA_REGISTRY_RESOURCE_EXTENSION_CLASS", "io.confluent.kafka.schemaregistry.security.SchemaRegistrySecurityResourceExtension,io.confluent.schema.exporter.SchemaExporterResourceExtension");
    envs.put("SCHEMA_REGISTRY_CONFLUENT_SCHEMA_REGISTRY_AUTHORIZER_CLASS", "io.confluent.kafka.schemaregistry.security.authorizer.rbac.RbacAuthorizer");
    envs.put("SCHEMA_REGISTRY_REST_SERVLET_INITIALIZOR_CLASSES", "io.confluent.common.security.jetty.initializer.InstallBearerOrBasicSecurityHandler");
    envs.put("SCHEMA_REGISTRY_PUBLIC_KEY_PATH", "/tmp/conf/public.pem");
    // Hardcoded values
    envs.put("SCHEMA_REGISTRY_CONFLUENT_METADATA_BOOTSTRAP_SERVER_URLS", "https://kafka1:8091,https://kafka2:8092");
    envs.put("SCHEMA_REGISTRY_CONFLUENT_METADATA_HTTP_AUTH_CREDENTIALS_PROVIDER", "BASIC");
    envs.put("SCHEMA_REGISTRY_CONFLUENT_METADATA_BASIC_AUTH_USER_INFO", "schemaregistryUser:schemaregistryUser");
    envs.put("SCHEMA_REGISTRY_PASSWORD_ENCODER_SECRET", "encoder-secret");
    envs.put("SCHEMA_REGISTRY_KAFKASTORE_UPDATE_HANDLERS", "io.confluent.schema.exporter.storage.SchemaExporterUpdateHandler");
    envs.put("CUB_CLASSPATH", "/usr/share/java/confluent-security/schema-registry/*:/usr/share/java/schema-registry/*:/usr/share/java/schema-registry-plugins/*:/usr/share/java/cp-base-new/*");
    return envs;
  }

  public Integer getPort() {
    return PORT;
  }
}
