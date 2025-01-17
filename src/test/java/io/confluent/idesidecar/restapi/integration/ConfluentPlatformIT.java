package io.confluent.idesidecar.restapi.integration;

import io.confluent.idesidecar.restapi.kafkarest.RecordsV3ErrorsSuite;
import io.confluent.idesidecar.restapi.kafkarest.RecordsV3Suite;
import io.confluent.idesidecar.restapi.kafkarest.RecordsV3WithoutSRSuite;
import io.confluent.idesidecar.restapi.kafkarest.api.TopicV3Suite;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.CPDemoTestEnvironment;
import io.confluent.idesidecar.restapi.util.TestEnvironment;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junitpioneer.jupiter.SetSystemProperty;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
// This could be used by tests to determine if they are running in the CP test environment
// and adjust their behavior.
@SetSystemProperty(key = "running-in-cp-test-environment", value = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ConfluentPlatformIT {

  /**
   * Use the <a href="https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers">Singleton Container</a>
   * pattern to ensure that the test environment is only started once, no matter how many
   * test classes extend this class. Testcontainers will assure that this is initialized once,
   * and stop the containers using the Ryuk container after all the tests have run.
   */
  private static final CPDemoTestEnvironment TEST_ENVIRONMENT = new CPDemoTestEnvironment();

  static {
    // Start up the test environment before any tests are run.
    // Let the Ryuk container handle stopping the container.
    TEST_ENVIRONMENT.start();
  }

  @AfterAll
  static void afterAll() {
    TEST_ENVIRONMENT.shutdown();
  }

  @Nested
  class DirectWithMutualTLSConnectionTests {

  }

  @Nested
  class DirectWithOauthConnectionTests {

  }

  @Nested
  class DirectWithBasicAuthConnectionTests {
    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class TopicTests extends AbstractIT implements TopicV3Suite {

      @Override
      public CPDemoTestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(this, environment().directConnectionBasicAuth());
      }
    }
  }

  @Nested
  class DirectWithSaslScramConnectionTests {
    @QuarkusIntegrationTest
    @Tag("io.confluent.common.utils.IntegrationTest")
    @TestProfile(NoAccessFilterProfile.class)
    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class TopicTests extends AbstractIT implements TopicV3Suite {

      @Override
      public CPDemoTestEnvironment environment() {
        return TEST_ENVIRONMENT;
      }

      @BeforeEach
      @Override
      public void setupConnection() {
        setupConnection(this, environment().directConnectionSaslScramAuth());
      }
    }
  }

  @QuarkusIntegrationTest
  @Tag("io.confluent.common.utils.IntegrationTest")
  @TestProfile(NoAccessFilterProfile.class)
  @Nested
  class WithoutSRRecordTests extends AbstractIT implements
      RecordsV3WithoutSRSuite {

    @Override
    public CPDemoTestEnvironment environment() {
      return TEST_ENVIRONMENT;
    }

    @BeforeEach
    @Override
    public void setupConnection() {
      setupConnection(this, environment().directConnectionSpecWithoutSR());
    }
  }

  @QuarkusIntegrationTest
  @Tag("io.confluent.common.utils.IntegrationTest")
  @TestProfile(NoAccessFilterProfile.class)
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class RecordTests extends AbstractIT implements
      RecordsV3Suite, RecordsV3ErrorsSuite {

    @Override
    public CPDemoTestEnvironment environment() {
      return TEST_ENVIRONMENT;
    }

    @BeforeEach
    @Override
    public void setupConnection() {
      setupConnection(this, TestEnvironment::directConnectionSpec);
    }
  }
}
