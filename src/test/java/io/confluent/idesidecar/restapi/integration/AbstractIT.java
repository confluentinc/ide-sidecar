package io.confluent.idesidecar.restapi.integration;

import static io.confluent.idesidecar.restapi.kafkarest.SchemaManager.SCHEMA_PROVIDERS;

import io.confluent.idesidecar.restapi.clients.ClientConfigurator;
import io.confluent.idesidecar.restapi.clients.SchemaErrors;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStates;
import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.util.LocalTestEnvironment;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.confluent.idesidecar.restapi.util.SidecarClient;
import io.confluent.idesidecar.restapi.util.TestEnvironment;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
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
 * <p>Most subclasses will reuse the same connection for all tests. Reusing the same connection in
 * multiple tests reduces the setup time for each test, as only the first test will need to create
 * the connection. (The sidecar will be terminated after all tests in the test class are run,
 * so deleting the connection is not necessary.)
 *
 * <p>Most of the {@link AbstractIT} concrete subclasses are defined so that all the tests will
 * run against the same connection. Each test class should call the
 * {@link #setupConnection(Class, Optional)} in it's {@code @BeforeEach} method to set up
 * connection, passing the test class and a {@link ConnectionSpec} instance from the
 * {@link TestEnvironment}, such as {@link TestEnvironment#localConnectionSpec()} or
 * {@link TestEnvironment#directConnectionSpec()}.
 *
 * <p>Other tests may want to use a different connection for each test. If the test class is
 * testing the Connections API, then the test class may want each <i>test method</i> to create
 * its own connection(s) via the {@link #setupConnection(String, ConnectionSpec)} methods,
 * verify the result of the creation, and when neceesary delete the connection.
 * The {@code @AfterEach} method should still call {@link #deleteAllConnections()} to clean up all
 * connections that remain at the end of each test, giving a clean slate to the next test method.
 */
public abstract class AbstractIT extends SidecarClient implements ITSuite {

  SchemaErrors.ConnectionId CONNECTION_1_ID = new SchemaErrors.ConnectionId("c1");

  protected AbstractIT(MessageViewerContext context) {
    this.context = new MessageViewerContext(
        null,
        null,
        null,
        null,
        null,
        CONNECTION_1_ID,
        "testClusterId",
        "testTopicName");
  }

  record ScopedConnection(
      ConnectionSpec spec,
      KafkaCluster kafkaCluster,
      SchemaRegistry srCluster,
      SimpleConsumer consumer
  ) {
    void useBy(SidecarClient client) {
      client.useConnection(spec.id());
      client.useClusters(kafkaCluster, srCluster);

      if (kafkaCluster != null) {
        client.setCurrentCluster(kafkaCluster.id());
      } else if (srCluster != null) {
        client.setCurrentCluster(srCluster.id());
      } else {
        throw new IllegalStateException("No cluster to use");
      }
    }
  }

  private static final Map<String, ScopedConnection> REUSABLE_CONNECTIONS_BY_TEST_SCOPE = new ConcurrentHashMap<>();

  protected ScopedConnection current;

  protected final MessageViewerContext context;

  @Inject
  RecordDeserializer recordDeserializer;
  SchemaErrors schemaErrors;


  @AfterEach
  public void afterEach() {
    // Delete all the content in the test environment, so it doesn't leak into other tests
    deleteAllContent();
  }

  /**
   * Get a {@link SimpleConsumer} that can be used to consume records from a Kafka topic,
   * using the current connection.
   *
   * @return the consumer
   */
  @Override
  public SimpleConsumer simpleConsumer() {
    return current.consumer();
  }

  /**
   * Create a {@link SimpleConsumer} that can be used to consume records from a Kafka topic.
   *
   * @param connection  the connection to use
   * @return the consumer
   */
  protected SimpleConsumer createSimpleConsumer(
      ConnectionState connection
  ) {
    var kafkaCluster = getKafkaCluster().orElseThrow();
    var sr = getSchemaRegistryCluster().orElse(null);
    var config = ClientConfigurator.getKafkaClientConfig(
        connection,
        kafkaCluster.bootstrapServers(),
        sr != null ? sr.uri() : null,
        false,
        null,
        Map.of()
    );
    var consumerProps = new Properties();
    var sidecarHost = sidecarHost();
    consumerProps.putAll(config);

    var consumer = new KafkaConsumer<>(
        consumerProps,
        new ByteArrayDeserializer(),
        new ByteArrayDeserializer()
    );

    var deserializer = new RecordDeserializer(
        1,
        1,
        10000,
        3,
        schemaErrors

    );
    CachedSchemaRegistryClient srClient = null;
    if (sr != null) {
      srClient = new CachedSchemaRegistryClient(
          Collections.singletonList(sidecarHost),
          10,
          SCHEMA_PROVIDERS,
          Collections.emptyMap(),
          Map.of(
              RequestHeadersConstants.CONNECTION_ID_HEADER, currentConnectionId(),
              RequestHeadersConstants.CLUSTER_ID_HEADER, sr.id()
          )
      );
    }
    return new SimpleConsumer(consumer, srClient, deserializer,context);
  }

  /**
   * Test classes that extend {@link AbstractIT} should call this method in their
   * {@code @BeforeEach} method to set up the connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases,
   * {@link #setupConnection(String, ConnectionSpec)} may be a better choice.
   *
   * @param testClassInstance      the test class
   * @param connectionSpecSupplier the supplier specification for the connection, which is
   *                               typically a reference to a method on {@link TestEnvironment}
   * @param <T>                    the type of {@link AbstractIT} subclass
   * @see #setupConnection(String, ConnectionSpec)
   * @see #setupConnection(AbstractIT, Optional)
   * @see TestEnvironment#localConnectionSpec()
   * @see TestEnvironment#directConnectionSpec()
   */
  protected <T extends AbstractIT> void setupConnection(
      T testClassInstance,
      Function<TestEnvironment, Optional<ConnectionSpec>> connectionSpecSupplier
  ) {
    setupConnection(testClassInstance.getClass(), connectionSpecSupplier.apply(environment()));
  }

  /**
   * Test classes that extend {@link AbstractIT} should call this method in their
   * {@code @BeforeEach} method to set up the connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases,
   * {@link #setupConnection(String, ConnectionSpec)} may be a better choice.
   *
   * @param testClassInstance the test class
   * @param connectionSpec    the specification for the connection, which is typically obtained
   *                          from the {@link TestEnvironment}
   * @param <T>               the type of {@link AbstractIT} subclass
   * @see #setupConnection(String, ConnectionSpec)
   * @see #setupConnection(AbstractIT, Function)
   * @see TestEnvironment#localConnectionSpec()
   * @see TestEnvironment#directConnectionSpec()
   */
  protected <T extends AbstractIT> void setupConnection(
      T testClassInstance,
      Optional<ConnectionSpec> connectionSpec
  ) {
    setupConnection(testClassInstance.getClass(), connectionSpec);
  }

  /**
   * Test classes that extend {@link AbstractIT} should call this method in their
   * {@code @BeforeEach} method to set up the connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases,
   * {@link #setupConnection(String, ConnectionSpec)} may be a better choice.
   *
   * @param testClass              the test class
   * @param connectionSpecSupplier the supplier specification for the connection, which is
   *                               typically a reference to a method on {@link TestEnvironment}
   * @param <T>                    the type of {@link AbstractIT} subclass
   * @see #setupConnection(String, ConnectionSpec)
   * @see TestEnvironment#localConnectionSpec()
   * @see TestEnvironment#directConnectionSpec()
   */
  protected <T> void setupConnection(
      Class<T> testClass,
      Function<TestEnvironment, Optional<ConnectionSpec>> connectionSpecSupplier
  ) {
    setupConnection(testClass.getSimpleName(), connectionSpecSupplier.apply(environment()).orElseThrow());
  }

  /**
   * Test classes that extend {@link AbstractIT} should call this method in their
   * {@code @BeforeEach} method to set up the connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases,
   * {@link #setupConnection(String, ConnectionSpec)} may be a better choice.
   *
   * @param testClass      the test class
   * @param connectionSpec the specification for the connection, which is typically obtained
   *                       from the {@link TestEnvironment}
   * @param <T>            the type of {@link AbstractIT} subclass
   * @see #setupConnection(String, ConnectionSpec)
   * @see TestEnvironment#localConnectionSpec()
   * @see TestEnvironment#directConnectionSpec()
   */
  protected <T> void setupConnection(
      Class<T> testClass,
      Optional<ConnectionSpec> connectionSpec
  ) {
    setupConnection(testClass.getSimpleName(), connectionSpec.orElseThrow());
  }

  /**
   * Test classes that extend {@link AbstractIT} should call this method in their
   * {@code @BeforeEach} method to set up the connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope, and
   * {@link #setupConnection(Class, Optional)} may be an easier way to do this.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases, this method may be a better choice.
   *
   * @param testScope      the test scope
   * @param connectionSpec the specification for the connection, which is typically obtained
   *                       from the {@link TestEnvironment}
   * @see #setupConnection(String, ConnectionSpec)
   */
  protected void setupConnection(
      String testScope,
      Optional<ConnectionSpec> connectionSpec
  ) {
    setupConnection(testScope, connectionSpec.orElseThrow());
  }

  /**
   * Test classes that extend {@link AbstractIT} should call this method in their
   * {@code @BeforeEach} method to set up the connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope, and
   * {@link #setupConnection(Class, Optional)} may be an easier way to do this.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases, this method may be a better choice.
   *
   * @param testScope              the test scope
   * @param connectionSpecSupplier the supplier specification for the connection, which is
   *                               typically a reference to a method on {@link TestEnvironment}
   * @see #setupConnection(String, ConnectionSpec)
   */
  protected void setupConnection(
      String testScope,
      Function<TestEnvironment, Optional<ConnectionSpec>> connectionSpecSupplier
  ) {
    setupConnection(testScope, connectionSpecSupplier.apply(environment()).orElseThrow());
  }

  /**
   * Test classes that extend {@link AbstractIT} should call this method in their
   * {@code @BeforeEach} method to set up the connection the test methods will use.
   *
   * <p>Each unique test scope will reuse the same connection, and most subclasses will
   * use the same test scope for all tests. In those cases, using the name of the subclass
   * is an easy way for tests to share the same scope, and
   * {@link #setupConnection(Class, Optional)} may be an easier way to do this.
   *
   * <p>Other test classes may need new/different connections for each test may want to use a
   * different test scope for each test. In those cases, this method may be a better choice.
   *
   * @param testScope      the test scope
   * @param connectionSpec the specification for the connection, which is typically obtained
   *                       from the {@link TestEnvironment}
   * @see #setupConnection(String, ConnectionSpec)
   */
  protected void setupConnection(String testScope, ConnectionSpec connectionSpec) {
    current = REUSABLE_CONNECTIONS_BY_TEST_SCOPE.computeIfAbsent(testScope, key -> {
      Log.debugf("Begin: Setting up %s connection", connectionSpec.type());

      // Append the scope to the name of the connection
      var spec = connectionSpec;
      spec = spec.withName( "%s (%s)".formatted(spec.name(), testScope));
      spec = spec.withId( "%s-%s".formatted(spec.id(), testScope));

      // Create the connection we'll use
      createConnection(spec);

      // Get the clusters we'll use
      var kafkaCluster = getKafkaCluster().orElse(null);
      var srCluster = getSchemaRegistryCluster().orElse(null);

      // Create the simple consumer
      var connection = ConnectionStates.from(connectionSpec, null);
      var simpleConsumer = createSimpleConsumer(connection);

      Log.debugf("End: Setting up %s connection", connectionSpec.type());
      return new ScopedConnection(spec, kafkaCluster, srCluster, simpleConsumer);
    });
    current.useBy(this);
  }

  @Override
  public void deleteAllConnections() {
    super.deleteAllConnections();
    REUSABLE_CONNECTIONS_BY_TEST_SCOPE.clear();
  }
}
