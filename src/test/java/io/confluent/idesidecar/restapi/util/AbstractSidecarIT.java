/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.util;

import static io.confluent.idesidecar.restapi.kafkarest.SchemaManager.SCHEMA_PROVIDERS;

import io.confluent.idesidecar.restapi.clients.SchemaErrors;
import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.quarkus.logging.Log;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;

/**
 * Abstract base class for integration tests that require a {@link TestEnvironment} and use
 * a sidecar client to interact with the Sidecar REST API.
 *
 * <h2>Sharing the {@link TestEnvironment}</h2>
 * <p>All subclasses of this class will share the same {@link LocalTestEnvironment} instance,
 * which is started before any tests are run and stopped after all tests have run.
 * Starting the test environment's containers takes 5-10 seconds, so doing it once
 * <i>for all integration tests</i> (that extend this class) helps the tests run faster.
 *
 * <h2>Sharing or not sharing connections</h2>
 * <p>Subclasses are expected to set up the sidecar connection(s) that they want their test
 * methods to use. Creating a connection may take several seconds, depending upon the effort
 * require to validate the connection and determine the status. This class provides a flexible
 * way for subclasses to control how connections are created and reused.
 *
 * <p>Most subclasses will use the same connection for all tests, often because the connection
 * is needed to test other APIs. In these cases, the test class' {@code @BeforeEach} method
 * should call {@link #setupLocalConnection(AbstractSidecarIT)} and/or
 * {@link #setupDirectConnection(AbstractSidecarIT)}.
 * This minimizes the setup time for each test, as only the first test will need to create the
 * (shared) connection. (The sidecar will be terminated after all tests in the test class are run,
 * so deleting the connection is not necessary.)
 *
 * <p>Other tests may want to use a different connection for each test. If the test class is
 * testing the Connections API, then the test class may want each test method to create its own
 * connections via the {@link #setupLocalConnection(String)} and/or
 * {@link #setupDirectConnection(String)} methods. The test method may also want to
 * delete the connections and verify the deletion. The {@code @AfterEach} method should still
 * call {@link #deleteAllConnections()} to clean up all connections that remain at the end of
 * each test, giving a clean slate to the next test method.
 *
 * <p>The {@link #setupLocalConnection(String)} and/or {@link #setupDirectConnection(String)}
 * give the test class more flexibility for defining the test scope. However, this is probably
 * less practical than using nested test classes each with their own {@code @BeforeEach}.
 */
public abstract class AbstractSidecarIT extends SidecarClient {

  /**
   * Use the <a href="https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers">Singleton Container</a>
   * pattern to ensure that the test environment is only started once, no matter how many
   * test classes extend this class. Testcontainers will assure that this is initialized once,
   * and stop the containers using the Ryuk container after all the tests have run.
   */
  private static final LocalTestEnvironment TEST_ENVIRONMENT = new LocalTestEnvironment();

  static {
    // Start up the test environment before any tests are run.
    // Let the Ryuk container handle stopping the container.
    TEST_ENVIRONMENT.start();
  }

  record ScopedConnection(
      String connectionId,
      SidecarClient.KafkaCluster kafkaCluster,
      SidecarClient.SchemaRegistry srCluster,
      SimpleConsumer simpleConsumer
  ) {
    void useBy(SidecarClient client) {
      client.useConnection(connectionId);
      client.useClusters(kafkaCluster, srCluster);
      client.setCurrentCluster(kafkaCluster.id());
    }
  }

  private static final Map<String, ScopedConnection> REUSABLE_CONNECTIONS_BY_TEST_SCOPE = new ConcurrentHashMap<>();

  protected ScopedConnection current;

  @AfterEach
  public void afterEach() {
    // Delete all the content in the test environment, so it doesn't leak into other tests
    deleteAllContent();
  }

  protected SimpleConsumer createSimpleConsumer(
      SidecarClient.KafkaCluster kafkaCluster,
      SidecarClient.SchemaRegistry srCluster
  ) {
    final SchemaErrors schemaErrors;
    // Setup a simple consumer
    var consumerProps = new Properties();
    var sidecarHost = sidecarHost();
    consumerProps.setProperty("bootstrap.servers", kafkaCluster.bootstrapServers());
    consumerProps.setProperty("schema.registry.url", srCluster.uri());
    return new SimpleConsumer(
        new KafkaConsumer<>(
            consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer()
        ),
        new CachedSchemaRegistryClient(
            Collections.singletonList(sidecarHost),
            10,
            SCHEMA_PROVIDERS,
            Collections.emptyMap(),
            Map.of(
                RequestHeadersConstants.CONNECTION_ID_HEADER, currentConnectionId(),
                RequestHeadersConstants.CLUSTER_ID_HEADER, srCluster.id()
            )
        ),
        new RecordDeserializer(
            1,
            1,
            10000,
            3,
            new SchemaErrors()
        )
    );
  }

  /**
   * Test classes that extend {@link AbstractSidecarIT} should call this method in their
   * {@code @BeforeEach} method to set up the local connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same local connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases, {@link #setupLocalConnection(String)}
   * may be a better choice.
   *
   * @param testClass the test class instance
   * @param <T> the type of {@link AbstractSidecarIT} subclass
   * @see #setupDirectConnection(String)
   * @see #setupDirectConnection(AbstractSidecarIT)
   * @see #setupLocalConnection(String)
   */
  protected <T extends AbstractSidecarIT> void setupLocalConnection(T testClass) {
    setupLocalConnection(testClass.getClass().getSimpleName());
  }

  /**
   * Test classes that extend {@link AbstractSidecarIT} should call this method in their
   * {@code @BeforeEach} method to set up the local connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same local connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope, and
   * {@link #setupLocalConnection(AbstractSidecarIT)} may be an easier way to do this.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases, this method may be a better choice.
   *
   * @see #setupDirectConnection(String)
   * @see #setupDirectConnection(AbstractSidecarIT)
   * @see #setupLocalConnection(AbstractSidecarIT)
   */
  protected void setupLocalConnection(String testScope) {
    current = REUSABLE_CONNECTIONS_BY_TEST_SCOPE.computeIfAbsent(testScope, key -> {
      Log.debug("Begin: Setting up local connection");
      // Create the local connection we'll use
      var connectionId = createLocalConnectionTo(TEST_ENVIRONMENT, testScope).id();

      // Get the clusters we'll use
      var kafkaCluster = getKafkaCluster().orElseThrow();
      var srCluster = getSchemaRegistryCluster().orElseThrow();

      // And create a simple consumer
      var simpleConsumer = createSimpleConsumer(kafkaCluster, srCluster);
      Log.debug("End: Setting up local connection");
      return new ScopedConnection(connectionId, kafkaCluster, srCluster, simpleConsumer);
    });
    current.useBy(this);
  }

  /**
   * Test classes that extend {@link AbstractSidecarIT} should call this method in their
   * {@code @BeforeEach} method to set up the local connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same local connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases, {@link #setupDirectConnection(String)}
   * may be a better choice.
   *
   * @param testClass the test class instance
   * @param <T> the type of {@link AbstractSidecarIT} subclass
   * @see #setupDirectConnection(String)
   * @see #setupLocalConnection(String)
   * @see #setupLocalConnection(AbstractSidecarIT)
   */
  protected <T extends AbstractSidecarIT> void setupDirectConnection(T testClass) {
    setupDirectConnection(testClass.getClass().getSimpleName());
  }

  /**
   * Test classes that extend {@link AbstractSidecarIT} should call this method in their
   * {@code @BeforeEach} method to set up the direct connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same local connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope, and
   * {@link #setupDirectConnection(AbstractSidecarIT)} may be an easier way to do this.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases, this method may be a better choice.
   *
   * @see #setupDirectConnection(AbstractSidecarIT)
   * @see #setupLocalConnection(AbstractSidecarIT)
   * @see #setupLocalConnection(String)
   */
  protected void setupDirectConnection(String testScope) {
    current = REUSABLE_CONNECTIONS_BY_TEST_SCOPE.computeIfAbsent(testScope, key -> {
      Log.debug("Begin: Setting up direct connection");
      // Create the direct connection we'll use
      var connectionId = createDirectConnectionTo(TEST_ENVIRONMENT, testScope).id();

      // Get the clusters we'll use
      var kafkaCluster = getKafkaCluster().orElseThrow();
      var srCluster = getSchemaRegistryCluster().orElseThrow();

      // And create a simple consumer
      var simpleConsumer = createSimpleConsumer(kafkaCluster, srCluster);
      Log.debug("End: Setting up local connection");
      return new ScopedConnection(connectionId, kafkaCluster, srCluster, simpleConsumer);
    });
    current.useBy(this);
  }

  protected SimpleConsumer simpleConsumer() {
    return current.simpleConsumer();
  }
}
