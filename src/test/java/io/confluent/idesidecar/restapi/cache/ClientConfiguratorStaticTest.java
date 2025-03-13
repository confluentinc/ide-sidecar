package io.confluent.idesidecar.restapi.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.clients.ClientConfigurator;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.credentials.ApiKeyAndSecret;
import io.confluent.idesidecar.restapi.credentials.ApiSecret;
import io.confluent.idesidecar.restapi.credentials.BasicCredentials;
import io.confluent.idesidecar.restapi.credentials.Credentials;
import io.confluent.idesidecar.restapi.credentials.Credentials.KafkaConnectionOptions;
import io.confluent.idesidecar.restapi.credentials.Credentials.SchemaRegistryConnectionOptions;
import io.confluent.idesidecar.restapi.credentials.KerberosCredentials;
import io.confluent.idesidecar.restapi.credentials.KerberosCredentialsBuilder;
import io.confluent.idesidecar.restapi.credentials.OAuthCredentials;
import io.confluent.idesidecar.restapi.credentials.Password;
import io.confluent.idesidecar.restapi.credentials.ScramCredentials;
import io.confluent.idesidecar.restapi.credentials.ScramCredentialsBuilder;
import io.confluent.idesidecar.restapi.credentials.TLSConfig;
import io.confluent.idesidecar.restapi.credentials.TLSConfig.KeyStore;
import io.confluent.idesidecar.restapi.credentials.TLSConfig.TrustStore;
import io.confluent.idesidecar.restapi.credentials.TLSConfigBuilder;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.SchemaRegistry;
import io.confluent.idesidecar.restapi.util.CCloud;
import java.io.StringReader;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mock;

class ClientConfiguratorStaticTest {

  static final String CONNECTION_ID = "connectionId";
  static final String KAFKA_CLUSTER_ID = "kafka-123";
  static final String SCHEMA_REGISTRY_ID = "sr-1234";
  static final String SCHEMA_REGISTRY_LSRC_ID = "lscr-1234";
  static final String BOOTSTRAP_SERVERS = "localhost:9092";
  static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
  static final String SCHEMA_REGISTRY_CCLOUD_URL = "https://psrc-1234.us-west-2.aws.confluent.cloud";

  static final String USERNAME = "user123";
  static final String PASSWORD = "my-secret";
  static final String API_KEY = "api-key-123";
  static final String API_SECRET = "api-secret-123";
  static final String OAUTH_TOKEN_URL = "http://localhost:8081/oauth/token";
  static final String OAUTH_CLIENT_ID = "client-123";
  static final String OAUTH_SCOPE = "oauth-scope";
  static final String OAUTH_SECRET = "oauth-secret";
  static final String MTLS_TRUSTSTORE_PATH = "/path/to/truststore";
  static final String MTLS_KEYSTORE_PATH = "/path/to/keystore";
  static final String MTLS_TRUSTSTORE_PASSWORD = "my-ts-secret";
  static final String MTLS_KEYSTORE_PASSWORD = "my-ks-secret";
  static final String MTLS_KEY_PASSWORD = "my-key-secret";

  static final BasicCredentials BASIC_CREDENTIALS = new BasicCredentials(
      USERNAME,
      new Password(PASSWORD.toCharArray())
  );
  static final OAuthCredentials OAUTH_CREDENTIALS = new OAuthCredentials(
      OAUTH_TOKEN_URL,
      OAUTH_CLIENT_ID,
      new Password(OAUTH_SECRET.toCharArray())
  );
  static final OAuthCredentials OAUTH_CREDENTIALS_WITH_SCOPE = new OAuthCredentials(
      OAUTH_TOKEN_URL,
      OAUTH_CLIENT_ID,
      new Password(OAUTH_SECRET.toCharArray()),
      OAUTH_SCOPE
  );
  static final ApiKeyAndSecret API_KEY_AND_SECRET = new ApiKeyAndSecret(
      API_KEY,
      new ApiSecret(API_SECRET.toCharArray())
  );
  static final TLSConfig ONE_WAY_TLS_CONFIG = new TLSConfig(
      MTLS_TRUSTSTORE_PATH,
      new Password(MTLS_TRUSTSTORE_PASSWORD.toCharArray())
  );
  static final TLSConfig DEFAULT_TLS_CONFIG = new TLSConfig();
  static final TLSConfig ONE_WAY_TLS_CONFIG_WITHOUT_HOSTNAME_VERIFICATION = ONE_WAY_TLS_CONFIG
      .with()
      .verifyHostname(false)
      .build();

  static final TLSConfig MUTUAL_TLS_CONFIG = new TLSConfig(
      MTLS_TRUSTSTORE_PATH,
      new Password(MTLS_TRUSTSTORE_PASSWORD.toCharArray()),
      MTLS_KEYSTORE_PATH,
      new Password(MTLS_KEYSTORE_PASSWORD.toCharArray()),
      new Password(MTLS_KEY_PASSWORD.toCharArray())
  );
  static final TLSConfig MUTUAL_TLS_CONFIG_WITH_TYPES = new TLSConfig(
      true,
      true,
      new TrustStore(
          MTLS_TRUSTSTORE_PATH,
          new Password(MTLS_TRUSTSTORE_PASSWORD.toCharArray()),
          TLSConfig.StoreType.JKS
      ),
      new KeyStore(
          MTLS_KEYSTORE_PATH,
          new Password(MTLS_KEYSTORE_PASSWORD.toCharArray()),
          TLSConfig.StoreType.JKS,
          new Password(MTLS_KEY_PASSWORD.toCharArray())
      )
  );

  static final TLSConfig TLS_DISABLED = TLSConfigBuilder
      .builder()
      .enabled(false)
      .build();

  static final ScramCredentials SCRAM_CREDENTIALS = ScramCredentialsBuilder
      .builder()
      .hashAlgorithm(ScramCredentials.HashAlgorithm.SCRAM_SHA_512)
      .username("scramUser")
      .password(new Password("scramPassword".toCharArray()))
      .build();

  static final ScramCredentials SCRAM_CREDENTIALS_WITH_DIFFERENT_ALGORITHM = ScramCredentialsBuilder
      .builder()
      .hashAlgorithm(ScramCredentials.HashAlgorithm.SCRAM_SHA_256)
      .username("scramUser02")
      .password(new Password("scramPassword256".toCharArray()))
      .build();

  static final TLSConfig HOSTNAME_VERIFICATION_DISABLED = TLSConfigBuilder
      .builder()
      // TLS is enabled but hostname verification is disabled
      .enabled(true)
      .verifyHostname(false)
      .build();

  static final KerberosCredentials KERBEROS_CREDENTIALS = KerberosCredentialsBuilder
      .builder()
      .principal("alice@EXAMPLE.com")
      .keytabPath("/etc/security/keytabs/alice.keytab")
      .build();

  static final KerberosCredentials KERBEROS_CREDENTIALS_WITH_SERVICE_NAME = KerberosCredentialsBuilder
      .builder()
      .principal("alice@EXAMPLE.com")
      .keytabPath("/etc/security/keytabs/alice.keytab")
      .serviceName("foobar")
      .build();

  @Mock
  ConnectionState connection;
  @Mock
  KafkaCluster kafka;
  @Mock
  SchemaRegistry schemaRegistry;
  @Mock
  SchemaRegistry ccloudSchemaRegistry;

  @BeforeEach
  void beforeEach() {
    connection = mock(ConnectionState.class);
    when(connection.getId()).thenReturn(CONNECTION_ID);

    kafka = mock(KafkaCluster.class);
    when(kafka.id()).thenReturn(KAFKA_CLUSTER_ID);
    when(kafka.bootstrapServers()).thenReturn(BOOTSTRAP_SERVERS);

    schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.id()).thenReturn(SCHEMA_REGISTRY_ID);
    when(schemaRegistry.uri()).thenReturn(SCHEMA_REGISTRY_URL);
    when(schemaRegistry.logicalId()).thenReturn(Optional.empty());

    ccloudSchemaRegistry = mock(SchemaRegistry.class);
    when(ccloudSchemaRegistry.id()).thenReturn(SCHEMA_REGISTRY_LSRC_ID);
    when(ccloudSchemaRegistry.uri()).thenReturn(SCHEMA_REGISTRY_CCLOUD_URL);
    when(ccloudSchemaRegistry.logicalId()).thenReturn(
        Optional.of(new CCloud.LsrcId(SCHEMA_REGISTRY_LSRC_ID)));
  }

  @TestFactory
  Stream<DynamicTest> shouldAllowCreateWithValidSpecsOrFailWithInvalidSpecsForSCRAM() {
    record TestInput(
        String displayName,
        KafkaCluster kafkaCluster,
        Credentials kafkaCredentials,
        TLSConfig kafkaTLSConfig,
        boolean redact,
        Duration timeout,
        String expectedKafkaConfig
    ) {

    }

    var inputs = Stream.of(
        new TestInput(
            "With SCRAM-SHA-512 credentials and plaintext",
            kafka,
            SCRAM_CREDENTIALS,
            TLS_DISABLED,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=SCRAM-SHA-512
                sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="scramUser" password="scramPassword";
                """
        ),
        new TestInput(
            "With SCRAM-SHA-256 credentials and plaintext",
            kafka,
            SCRAM_CREDENTIALS_WITH_DIFFERENT_ALGORITHM,
            TLS_DISABLED,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=SCRAM-SHA-256
                sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="scramUser02" password="scramPassword256";
                """
        )
    );

    return inputs.map(input -> DynamicTest.dynamicTest(
        "Testing: " + input.displayName,
        () -> {
          assertNotNull(input.expectedKafkaConfig);
          var expectedKafkaConfig = loadProperties(input.expectedKafkaConfig);

          expectGetKafkaCredentialsFromConnection(input.kafkaCredentials);
          expectGetKafkaTLSConfigFromConnection(input.kafkaTLSConfig);
          var options = new KafkaConnectionOptions(input.redact, input.kafkaTLSConfig);
          expectGetKafkaConnectionOptions(options);

          var kafkaConfig = ClientConfigurator.getKafkaClientConfig(
              connection,
              input.kafkaCluster.bootstrapServers(),
              null,
              input.redact,
              input.timeout,
              Map.of()
          );
          assertMapsEquals(
              expectedKafkaConfig,
              kafkaConfig,
              "Expected Kafka config to match for '%s' test case".formatted(input.displayName)
          );
        }
    ));
  }

  @TestFactory
  Stream<DynamicTest> shouldAllowCreateWithValidSpecsOrFailWithInvalidSpecs() {
    record TestInput(
        String displayName,
        KafkaCluster kafkaCluster,
        Credentials kafkaCredentials,
        SchemaRegistry schemaRegistry,
        Credentials srCredentials,
        TLSConfig kafkaTLSConfig,
        TLSConfig schemaRegistryTLSConfig,
        boolean redact,
        Duration timeout,
        String expectedKafkaConfig,
        String expectedSchemaRegistryConfig
    ) {

    }
    var inputs = Stream.of(
        new TestInput(
            "No credentials",
            kafka,
            null,
            schemaRegistry,
            null,
            TLS_DISABLED,
            TLS_DISABLED,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                """,
            """
                schema.registry.url=http://localhost:8081
                """
        ),
        new TestInput(
            "No credentials with just Kafka",
            kafka,
            null,
            null,
            null,
            TLS_DISABLED,
            TLS_DISABLED,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                """,
            null
        ),
        new TestInput(
            "No credentials, just Kafka and default TLS",
            kafka,
            null,
            null,
            null,
            DEFAULT_TLS_CONFIG,
            null,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SSL
                """,
            null
        ),
        new TestInput(
            "No credentials and TLS",
            kafka,
            null,
            schemaRegistry,
            null,
            ONE_WAY_TLS_CONFIG,
            ONE_WAY_TLS_CONFIG,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SSL
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                """.formatted(MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                """.formatted(MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD)
        ),
        new TestInput(
            "With basic credentials and plaintext",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            TLS_DISABLED,
            TLS_DISABLED,
            false,
            Duration.ofSeconds(10),
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                """.formatted(USERNAME, PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                schema.registry.request.timeout.ms=10000
                """.formatted(USERNAME, PASSWORD)
        ),
        new TestInput(
            "With basic credentials and plaintext but redacted",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            TLS_DISABLED,
            TLS_DISABLED,
            true,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="********";
                """.formatted(USERNAME),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:********
                """.formatted(USERNAME)
        ),
        new TestInput(
            "With basic credentials and TLS",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            ONE_WAY_TLS_CONFIG,
            ONE_WAY_TLS_CONFIG,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                """.formatted(USERNAME, PASSWORD, MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                """.formatted(USERNAME, PASSWORD, MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD)
        ),
        new TestInput(
            "With basic credentials and Mutual TLS",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            MUTUAL_TLS_CONFIG,
            MUTUAL_TLS_CONFIG,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                ssl.keystore.location=%s
                ssl.keystore.password=%s
                ssl.key.password=%s
                """
                .formatted(
                    USERNAME,
                    PASSWORD,
                    MTLS_TRUSTSTORE_PATH,
                    MTLS_TRUSTSTORE_PASSWORD,
                    MTLS_KEYSTORE_PATH,
                    MTLS_KEYSTORE_PASSWORD,
                    MTLS_KEY_PASSWORD
                ),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                ssl.keystore.location=%s
                ssl.keystore.password=%s
                ssl.key.password=%s
                """
                .formatted(
                    USERNAME,
                    PASSWORD,
                    MTLS_TRUSTSTORE_PATH,
                    MTLS_TRUSTSTORE_PASSWORD,
                    MTLS_KEYSTORE_PATH,
                    MTLS_KEYSTORE_PASSWORD,
                    MTLS_KEY_PASSWORD
                )
        ),
        new TestInput(
            "With basic credentials and Mutual TLS with explicit types",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            MUTUAL_TLS_CONFIG_WITH_TYPES,
            MUTUAL_TLS_CONFIG_WITH_TYPES,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                ssl.truststore.type=JKS
                ssl.keystore.location=%s
                ssl.keystore.password=%s
                ssl.keystore.type=JKS
                ssl.key.password=%s
                """
                .formatted(
                    USERNAME,
                    PASSWORD,
                    MTLS_TRUSTSTORE_PATH,
                    MTLS_TRUSTSTORE_PASSWORD,
                    MTLS_KEYSTORE_PATH,
                    MTLS_KEYSTORE_PASSWORD,
                    MTLS_KEY_PASSWORD
                ),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                ssl.truststore.type=JKS
                ssl.keystore.location=%s
                ssl.keystore.password=%s
                ssl.keystore.type=JKS
                ssl.key.password=%s
                """
                .formatted(
                    USERNAME,
                    PASSWORD,
                    MTLS_TRUSTSTORE_PATH,
                    MTLS_TRUSTSTORE_PASSWORD,
                    MTLS_KEYSTORE_PATH,
                    MTLS_KEYSTORE_PASSWORD,
                    MTLS_KEY_PASSWORD
                )
        ),
        new TestInput(
            "With basic credentials and TLS but redacted",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            ONE_WAY_TLS_CONFIG,
            ONE_WAY_TLS_CONFIG,
            true,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="********";
                ssl.truststore.location=%s
                ssl.truststore.password=********
                """.formatted(USERNAME, MTLS_TRUSTSTORE_PATH),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:********
                ssl.truststore.location=%s
                ssl.truststore.password=********
                """.formatted(USERNAME, MTLS_TRUSTSTORE_PATH)
        ),
        new TestInput(
            "With basic credentials and TLS and disable server hostname verification",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            ONE_WAY_TLS_CONFIG_WITHOUT_HOSTNAME_VERIFICATION,
            ONE_WAY_TLS_CONFIG_WITHOUT_HOSTNAME_VERIFICATION,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                ssl.endpoint.identification.algorithm=
                """.formatted(USERNAME, PASSWORD, MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                ssl.endpoint.identification.algorithm=
                """.formatted(USERNAME, PASSWORD, MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD)
        ),
        new TestInput(
            "With mixed credentials and TLS",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            API_KEY_AND_SECRET,
            ONE_WAY_TLS_CONFIG,
            ONE_WAY_TLS_CONFIG,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                """.formatted(USERNAME, PASSWORD, MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                """.formatted(API_KEY, API_SECRET, MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD)
        ),
        new TestInput(
            "With different TLS configs for Kafka and SR",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            ONE_WAY_TLS_CONFIG,
            MUTUAL_TLS_CONFIG,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                """.formatted(USERNAME, PASSWORD, MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                ssl.keystore.location=%s
                ssl.keystore.password=%s
                ssl.key.password=%s
                """
                .formatted(
                    USERNAME,
                    PASSWORD,
                    MTLS_TRUSTSTORE_PATH,
                    MTLS_TRUSTSTORE_PASSWORD,
                    MTLS_KEYSTORE_PATH,
                    MTLS_KEYSTORE_PASSWORD,
                    MTLS_KEY_PASSWORD
                )
        ),
        new TestInput(
            "With OAuth for Kafka and SR",
            kafka,
            OAUTH_CREDENTIALS,
            schemaRegistry,
            OAUTH_CREDENTIALS,
            TLS_DISABLED,
            TLS_DISABLED,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=OAUTHBEARER
                sasl.oauthbearer.token.endpoint.url=http://localhost:8081/oauth/token
                sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler
                sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="%s" clientSecret="%s";
                """.formatted(OAUTH_CLIENT_ID, OAUTH_SECRET),
            """
                schema.registry.url=http://localhost:8081
                bearer.auth.credentials.source=OAUTHBEARER
                bearer.auth.issuer.endpoint.url=http://localhost:8081/oauth/token
                bearer.auth.client.id=%s
                bearer.auth.client.secret=%s
                """.formatted(OAUTH_CLIENT_ID, OAUTH_SECRET)
        ),
        new TestInput(
            "With OAuth for Kafka and SR over TLS",
            kafka,
            OAUTH_CREDENTIALS,
            schemaRegistry,
            OAUTH_CREDENTIALS,
            ONE_WAY_TLS_CONFIG,
            ONE_WAY_TLS_CONFIG,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=OAUTHBEARER
                sasl.oauthbearer.token.endpoint.url=http://localhost:8081/oauth/token
                sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler
                sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="%s" clientSecret="%s";
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                """.formatted(
                OAUTH_CLIENT_ID, OAUTH_SECRET, MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                bearer.auth.credentials.source=OAUTHBEARER
                bearer.auth.issuer.endpoint.url=http://localhost:8081/oauth/token
                bearer.auth.client.id=%s
                bearer.auth.client.secret=%s
                ssl.truststore.location=%s
                ssl.truststore.password=%s
                """.formatted(
                OAUTH_CLIENT_ID, OAUTH_SECRET, MTLS_TRUSTSTORE_PATH, MTLS_TRUSTSTORE_PASSWORD)
        ),
        new TestInput(
            "With OAuth with scopes for Kafka and SR",
            kafka,
            OAUTH_CREDENTIALS_WITH_SCOPE,
            schemaRegistry,
            OAUTH_CREDENTIALS_WITH_SCOPE,
            TLS_DISABLED,
            TLS_DISABLED,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=OAUTHBEARER
                sasl.oauthbearer.token.endpoint.url=http://localhost:8081/oauth/token
                sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler
                sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="%s" clientSecret="%s" scope="%s";
                """.formatted(OAUTH_CLIENT_ID, OAUTH_SECRET, OAUTH_SCOPE),
            """
                schema.registry.url=http://localhost:8081
                bearer.auth.credentials.source=OAUTHBEARER
                bearer.auth.issuer.endpoint.url=http://localhost:8081/oauth/token
                bearer.auth.client.id=%s
                bearer.auth.client.secret=%s
                bearer.auth.scope=%s
                """.formatted(OAUTH_CLIENT_ID, OAUTH_SECRET, OAUTH_SCOPE)
        ),
        new TestInput(
            "With OAuth for Kafka and SR and disable server hostname verification",
            kafka,
            OAUTH_CREDENTIALS,
            schemaRegistry,
            OAUTH_CREDENTIALS,
            HOSTNAME_VERIFICATION_DISABLED,
            HOSTNAME_VERIFICATION_DISABLED,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                ssl.endpoint.identification.algorithm=
                sasl.mechanism=OAUTHBEARER
                sasl.oauthbearer.token.endpoint.url=http://localhost:8081/oauth/token
                sasl.login.callback.handler.class=org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler
                sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="%s" clientSecret="%s";
                """.formatted(OAUTH_CLIENT_ID, OAUTH_SECRET),
            """
                schema.registry.url=http://localhost:8081
                bearer.auth.credentials.source=OAUTHBEARER
                bearer.auth.issuer.endpoint.url=http://localhost:8081/oauth/token
                bearer.auth.client.id=%s
                bearer.auth.client.secret=%s
                ssl.endpoint.identification.algorithm=
                """.formatted(OAUTH_CLIENT_ID, OAUTH_SECRET)
        ),
        new TestInput(
            "With Kerberos for Kafka over SSL, no SR",
            kafka,
            KERBEROS_CREDENTIALS,
            null,
            null,
            DEFAULT_TLS_CONFIG,
            null,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=GSSAPI
                sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true doNotPrompt=true useTicketCache=false keyTab="/etc/security/keytabs/alice.keytab" principal="alice@EXAMPLE.com";
                """,
            null
        ),
        new TestInput(
            "With Kerberos for Kafka over SSL with a custom broker service name, no SR",
            kafka,
            KERBEROS_CREDENTIALS_WITH_SERVICE_NAME,
            null,
            null,
            DEFAULT_TLS_CONFIG,
            null,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=GSSAPI
                sasl.jaas.config=com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true doNotPrompt=true useTicketCache=false keyTab="/etc/security/keytabs/alice.keytab" principal="alice@EXAMPLE.com";
                sasl.kerberos.service.name=foobar
                """,
            null
        )
    );
    return inputs
        .map(input -> DynamicTest.dynamicTest(
            "Testing: " + input.displayName,
            () -> {
              assertNotNull(input.expectedKafkaConfig);
              var expectedKafkaConfig = loadProperties(input.expectedKafkaConfig);

              expectGetKafkaCredentialsFromConnection(input.kafkaCredentials);
              expectGetSchemaRegistryCredentialsFromConnection(input.srCredentials);
              expectGetKafkaTLSConfigFromConnection(input.kafkaTLSConfig);
              expectGetSchemaRegistryTLSConfigFromConnection(input.schemaRegistryTLSConfig);
              var options = new KafkaConnectionOptions(input.redact, input.kafkaTLSConfig);
              expectGetKafkaConnectionOptions(options);
              var srOptions = new SchemaRegistryConnectionOptions(
                  input.redact, input.schemaRegistryTLSConfig
              );
              expectGetSchemaRegistryConnectionOptions(srOptions);

              // The Kafka config without SR should match
              var kafkaConfig = ClientConfigurator.getKafkaClientConfig(
                  connection,
                  input.kafkaCluster.bootstrapServers(),
                  null,
                  input.redact,
                  input.timeout,
                  Map.of()
              );
              assertMapsEquals(
                  expectedKafkaConfig,
                  kafkaConfig,
                  "Expected Kafka config to match for '%s' test case".formatted(input.displayName)
              );
              if (input.schemaRegistry != null) {
                assertNotNull(input.expectedSchemaRegistryConfig);
                // The Schema Registry config should match
                var expectedSchemaRegistryConfig = loadProperties(
                    input.expectedSchemaRegistryConfig);
                var srConfig = ClientConfigurator.getSchemaRegistryClientConfig(
                    connection,
                    input.schemaRegistry.uri(),
                    input.redact,
                    input.timeout
                );
                assertMapsEquals(
                    expectedSchemaRegistryConfig,
                    srConfig,
                    "Expected Schema Registry config to match for '%s' test case".formatted(
                        input.displayName)
                );

                // And the kafka config with SR matches
                var expectedKafkaConfigWithSr = new HashMap<>(expectedKafkaConfig);
                expectedSchemaRegistryConfig.forEach((k, v) -> {
                  var prefix = k.startsWith("schema.registry.") ? "" : "schema.registry.";
                  expectedKafkaConfigWithSr.put(prefix + k, v);
                });
                var kafkaConfigWithSr = ClientConfigurator.getKafkaClientConfig(
                    connection,
                    input.kafkaCluster.bootstrapServers(),
                    input.schemaRegistry.uri(),
                    input.redact,
                    input.timeout,
                    Map.of()
                );
                assertMapsEquals(
                    expectedKafkaConfigWithSr,
                    kafkaConfigWithSr,
                    "Expected Kafka config with SR to match for '%s' test case".formatted(
                        input.displayName)
                );
              }
            }
        ));
  }

  Map<String, Object> loadProperties(String value) {
    var properties = new Properties();
    try {
      properties.load(new StringReader(value));
    } catch (Exception e) {
      fail("Failed to load properties from string: %s".formatted(value));
    }
    var result = new LinkedHashMap<String, Object>();
    properties.forEach((k, v) -> result.put((String) k, v));
    return result;
  }

  void expectGetKafkaConnectionOptions(KafkaConnectionOptions options) {
    when(connection.getKafkaConnectionOptions())
        .thenReturn(options);
  }

  void expectGetSchemaRegistryConnectionOptions(SchemaRegistryConnectionOptions options) {
    when(connection.getSchemaRegistryOptions())
        .thenReturn(options);
  }

  void expectGetKafkaCredentialsFromConnection(Credentials credentials) {
    when(connection.getKafkaCredentials())
        .thenReturn(Optional.ofNullable(credentials));
  }

  void expectGetSchemaRegistryCredentialsFromConnection(Credentials credentials) {
    when(connection.getSchemaRegistryCredentials())
        .thenReturn(Optional.ofNullable(credentials));
  }

  void expectGetKafkaTLSConfigFromConnection(TLSConfig tlsConfig) {
    when(connection.getKafkaTLSConfig())
        .thenReturn(Optional.ofNullable(tlsConfig));
  }

  void expectGetSchemaRegistryTLSConfigFromConnection(TLSConfig tlsConfig) {
    when(connection.getSchemaRegistryTLSConfig())
        .thenReturn(Optional.ofNullable(tlsConfig));
  }

  void assertMapsEquals(Map<String, ?> expected, Map<String, ?> actual, String message) {
    expected.forEach((k, v) -> {
      var actualValue = actual.get(k);
      assertNotNull(actualValue, "%s: expected key '%s' to be present".formatted(message, k));
      assertEquals(v.toString(), actualValue.toString(),
          "%s: expected value for key '%s' to match '%s' but was '%s'".formatted(message, k, v,
              actualValue));
    });
    assertEquals(expected.size(), actual.size(),
        "%s: expected %d entries but found %d".formatted(message, expected.size(), actual.size()));
  }
}