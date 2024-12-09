package io.confluent.idesidecar.restapi.integration;

import io.confluent.idesidecar.restapi.util.LocalTestEnvironment;
import org.junit.jupiter.api.Nested;

public class ConfluentPlatformIT {

  /**
   * Use the <a
   * href="https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers">Singleton
   * Container</a> pattern to ensure that the test environment is only started once, no matter how
   * many test classes extend this class. Testcontainers will assure that this is initialized once,
   * and stop the containers using the Ryuk container after all the tests have run.
   */
  private static final LocalTestEnvironment TEST_ENVIRONMENT = new LocalTestEnvironment();

  static {
    // Start up the test environment before any tests are run.
    // Let the Ryuk container handle stopping the container.
    TEST_ENVIRONMENT.start();
  }


  @Nested
  class DirectWithMutualTLSConnectionTests {

  }

  @Nested
  class DirectWithOauthConnectionTests {

  }


}
