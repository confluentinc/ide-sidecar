package io.confluent.idesidecar.restapi.integration;

import io.confluent.idesidecar.restapi.integration.connection.DirectConnectionSuite;
import io.confluent.idesidecar.restapi.integration.connection.LocalConnectionSuite;
import io.confluent.idesidecar.restapi.kafkarest.RecordsV3DryRunSuite;
import io.confluent.idesidecar.restapi.kafkarest.RecordsV3ErrorsSuite;
import io.confluent.idesidecar.restapi.kafkarest.RecordsV3Suite;
import io.confluent.idesidecar.restapi.kafkarest.RecordsV3WithoutSRSuite;
import io.confluent.idesidecar.restapi.kafkarest.api.ClusterV3Suite;
import io.confluent.idesidecar.restapi.kafkarest.api.PartitionV3Suite;
import io.confluent.idesidecar.restapi.kafkarest.api.TopicConfigV3Suite;
import io.confluent.idesidecar.restapi.kafkarest.api.TopicV3Suite;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumerSuite;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.resources.KafkaConsumeSuite;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.LocalTestEnvironment;
import io.confluent.idesidecar.restapi.util.TestEnvironment;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;

public class LocalIT {

  private static final LocalTestEnvironment TEST_ENVIRONMENT = new LocalTestEnvironment();

  @BeforeAll
  static void beforeAll() {
    // Start the test environment and use it for all tests
    TEST_ENVIRONMENT.start();
  }

  @AfterAll
  static void afterAll() {
    // Shutdown the test environment after all tests have run
    TEST_ENVIRONMENT.shutdown();
  }

  @QuarkusIntegrationTest
  @Tag("io.confluent.common.utils.IntegrationTest")
  @TestProfile(NoAccessFilterProfile.class)
  @Nested
  class ConnectionTests extends AbstractIT implements DirectConnectionSuite, LocalConnectionSuite {

    @Override
    public TestEnvironment environment() {
      return TEST_ENVIRONMENT;
    }

    @BeforeEach
    @Override
    public void setupConnection() {
      deleteAllConnections();
    }
  }

  @Nested
  class LocalConnectionTests {

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class RecordTests extends AbstractIT implements RecordsV3Suite, RecordsV3DryRunSuite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(this, TestEnvironment::localConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class RecordTestsWithoutSR extends AbstractIT implements RecordsV3WithoutSRSuite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(this, Optional.of(
            ConnectionSpec.createLocal(
                "local-connection-without-sr",
                "Local connection without Schema Registry",
                // Disable Schema Registry
                new ConnectionSpec.LocalConfig("")
            )
        ));
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class RecordFailureTests extends AbstractIT implements RecordsV3ErrorsSuite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(this, TestEnvironment::localConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class TopicTests extends AbstractIT implements TopicV3Suite, PartitionV3Suite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(this, TestEnvironment::localConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class TopicConfigTests extends AbstractIT implements TopicConfigV3Suite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(this, TestEnvironment::localConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class ClustersTests extends AbstractIT implements ClusterV3Suite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(this, TestEnvironment::localConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class ConsumptionTests extends AbstractIT implements SimpleConsumerSuite, KafkaConsumeSuite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(this, TestEnvironment::localConnectionSpec);
      }
    }
  }

  @Nested
  class DirectConnectionWithoutCredentialsTests {

    /**
     * All tests that create connections with this scope will reuse the same connection.
     */
    static final String CONNECTION_SCOPE = DirectConnectionWithoutCredentialsTests.class.getName();

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class RecordTests extends AbstractIT implements RecordsV3Suite, RecordsV3DryRunSuite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(CONNECTION_SCOPE, TestEnvironment::directConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class RecordFailureTests extends AbstractIT implements RecordsV3ErrorsSuite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(CONNECTION_SCOPE, TestEnvironment::directConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class TopicTests extends AbstractIT implements TopicV3Suite, PartitionV3Suite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(CONNECTION_SCOPE, TestEnvironment::directConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class TopicConfigTests extends AbstractIT implements TopicConfigV3Suite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(CONNECTION_SCOPE, TestEnvironment::directConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class ClustersTests extends AbstractIT implements ClusterV3Suite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(CONNECTION_SCOPE, TestEnvironment::directConnectionSpec);
      }
    }

    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    class ConsumptionTests extends AbstractIT implements SimpleConsumerSuite, KafkaConsumeSuite {

      @Override
      public TestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(CONNECTION_SCOPE, TestEnvironment::directConnectionSpec);
      }
    }

  }
}
