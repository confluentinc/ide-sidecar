package io.confluent.idesidecar.restapi.integration;

import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.util.SidecarClientApi;
import io.confluent.idesidecar.restapi.util.TestEnvironment;
import java.util.Map;

/**
 * An interface that defines the test methods for a suite of functional tests to be run against the
 * <i>current test environment</i> and a <i>current connection</i> in the running sidecar instance.
 *
 * <p>The {@link SidecarClientApi} provides the basic methods to interact with the sidecar REST
 * API.
 * The {@link #environment()} method provides access to the current test environment, which can be
 * used to directly access the Kafka cluster, Schema Registry, and any other resources in the system
 * under test.
 *
 * <p>Integration tests will define subclasses of {@link AbstractIT} that implement one or more
 * interfaces that extend this {@link ITSuite} interface, to run those tests against the the
 * {@link TestEnvironment} and one or more connections. These classes will implement the
 * {@link #environment()} method and will implement thee {@link #setupConnection()} method to set up
 * the current connection to be used by all test methods in the suite. The
 * {@link #setupConnection()} implementation can use the various methods on {@link TestEnvironment},
 * like {@link TestEnvironment#localConnectionSpec()} or
 * {@link TestEnvironment#directConnectionSpec(boolean)}, that return
 * {@link io.confluent.idesidecar.restapi.models.ConnectionSpec} instances that can then be used to
 * {@link #createConnection(ConnectionSpec) create connections in the sidecar}.
 */
public interface ITSuite extends SidecarClientApi {

  /**
   * Get the current {@link TestEnvironment}, which may provide direct access to the Kafka cluster,
   * Schema Registry, and any other resources in the system under test.
   *
   * @return the TestEnvironment, never null
   */
  TestEnvironment environment();

  /**
   * Get a {@link SimpleConsumer} for the {@link #getKafkaCluster() current Kafka cluster} and
   * configured with the {@link #getSchemaRegistryCluster() current Schema Registry} both running in
   * the {@link #environment() current test environment}.
   *
   * <p>This allows tests to consume messages directly from Kafka topics (and if needed deserialize
   * them using schemas from the Schema Registry) without going through the sidecar REST API, to
   * verify that the sidecar is behaving correctly.
   *
   * <p>This really needs to be used only by tests that verify the sidecar produces and consumes
   * records correctly. Other tests can then rely upon the sidecar APIs to produce and consume
   * correctly.
   *
   * @return the consumer, never null
   */
  SimpleConsumer simpleConsumer();

  Map<String, Object> kafkaClientConfig();

  /**
   * Hook that allows the integration test to set up the
   * {@link #useConnection(String) current connection} that will be used by all test methods in
   * {@link ITSuite} sub-interfaces.
   */
  void setupConnection();
}
