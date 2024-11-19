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
import io.confluent.idesidecar.restapi.credentials.Password;
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
  static final String SCHEMA_REGISTRY_CCLOUD_URL = "https://psrc-1234.us-west-2.aws.confluent"
                                                   + ".cloud";

  static final String USERNAME = "user123";
  static final String PASSWORD = "my-secret";
  static final String API_KEY = "api-key-123";
  static final String API_SECRET = "api-secret-123";

  static final BasicCredentials BASIC_CREDENTIALS = new BasicCredentials(
      USERNAME,
      new Password(PASSWORD.toCharArray())
  );
  static final ApiKeyAndSecret API_KEY_AND_SECRET = new ApiKeyAndSecret(
      API_KEY,
      new ApiSecret(API_SECRET.toCharArray())
  );

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
  Stream<DynamicTest> shouldAllowCreateWithValidSpecsOrFailWithInvalidSpecs() {
    record TestInput(
        String displayName,
        KafkaCluster kafkaCluster,
        Credentials kafkaCredentials,
        SchemaRegistry schemaRegistry,
        Credentials srCredentials,
        boolean ssl,
        boolean verifyUnsignedCertificates,
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
            true,
            true,
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
            true,
            true,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                """,
            null
        ),
        new TestInput(
            "With basic credentials and plaintext",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            false,
            false,
            false,
            Duration.ofSeconds(10),
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                ssl.endpoint.identification.algorithm=
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
            false,
            false,
            true,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_PLAINTEXT
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="********";
                ssl.endpoint.identification.algorithm=
                                
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
            true,
            true,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                """.formatted(USERNAME, PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                """.formatted(USERNAME, PASSWORD)
        ),
        new TestInput(
            "With basic credentials and TLS but redacted",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            true,
            true,
            true,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
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
            "With basic credentials and TLS and verify hostnames",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            BASIC_CREDENTIALS,
            true,
            true,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                """.formatted(USERNAME, PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                """.formatted(USERNAME, PASSWORD)
        ),
        new TestInput(
            "With mixed credentials and TLS",
            kafka,
            BASIC_CREDENTIALS,
            schemaRegistry,
            API_KEY_AND_SECRET,
            true,
            true,
            false,
            null,
            """
                bootstrap.servers=localhost:9092
                security.protocol=SASL_SSL
                sasl.mechanism=PLAIN
                sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                """.formatted(USERNAME, PASSWORD),
            """
                schema.registry.url=http://localhost:8081
                basic.auth.credentials.source=USER_INFO
                basic.auth.user.info=%s:%s
                """.formatted(API_KEY, API_SECRET)
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
              var options = new KafkaConnectionOptions(
                  input.ssl,
                  input.verifyUnsignedCertificates,
                  input.redact
              );
              expectGetKafkaConnectionOptions(options);

              // The Kafka config without SR should match
              var kafkaConfig = ClientConfigurator.getKafkaClientConfig(
                  connection,
                  input.kafkaCluster.id(),
                  input.kafkaCluster.bootstrapServers(),
                  null,
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
                    input.schemaRegistry.id(),
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
                    input.kafkaCluster.id(),
                    input.kafkaCluster.bootstrapServers(),
                    input.schemaRegistry.id(),
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

  void expectGetKafkaCredentialsFromConnection(Credentials credentials) {
    when(connection.getKafkaCredentials())
        .thenReturn(Optional.ofNullable(credentials));
  }

  void expectGetSchemaRegistryCredentialsFromConnection(Credentials credentials) {
    when(connection.getSchemaRegistryCredentials())
        .thenReturn(Optional.ofNullable(credentials));
  }

  void assertMapsEquals(Map<String, ?> expected, Map<String, ?> actual, String message) {
    expected.forEach((k, v) -> {
      var actualValue = actual.get(k);
      assertNotNull(actualValue, "%s: expected key '%s' to be present".formatted(message, k));
      assertEquals(
          v.toString(), actualValue.toString(),
          "%s: expected value for key '%s' to match '%s' but was '%s'".formatted(message, k, v,
              actualValue
          )
      );
    });
    assertEquals(
        expected.size(), actual.size(),
        "%s: expected %d entries but found %d".formatted(message, expected.size(), actual.size())
    );
  }
}