package io.confluent.idesidecar.restapi.integration;

import io.confluent.idesidecar.restapi.kafkarest.api.ClusterV3Suite;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.SHA256Digest;
import io.confluent.idesidecar.restapi.util.TestEnvironment;
import io.confluent.idesidecar.restapi.util.WarpStreamTestEnvironment;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;

/**
 * Regression tests to be run against a local WarpStream container.
 */
public class WarpStreamRegressionIT {

  private static final WarpStreamTestEnvironment TEST_ENVIRONMENT = new WarpStreamTestEnvironment(
      // This is a WarpStream version that is known to return `null` as part of the
      // `DescribeCluster.controller()` response. The sidecar's Internal Kafka REST
      // previously failed to handle this case correctly. This test class ensures that
      // the ClusterV3Suite passes since the sidecar now correctly handles this case.
      new SHA256Digest("dc694b4ecb415b61264707a87da1b440b3178ee55283c5a38b9c59c1d856b819")
  );

  static {
    TEST_ENVIRONMENT.start();
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
      setupConnection(this, TestEnvironment::directConnectionSpec);
    }
  }
}
