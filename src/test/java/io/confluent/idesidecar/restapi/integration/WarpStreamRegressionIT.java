package io.confluent.idesidecar.restapi.integration;

import io.confluent.idesidecar.restapi.kafkarest.api.ClusterV3Suite;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
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
      // Pin the stable channel rather than `latest`: `latest` is a moving tag that tracks the
      // newest (possibly pre-release) agent, so a republish can break CI on every branch at once.
      // This suite verifies that the sidecar's Internal Kafka REST (the ClusterV3Suite) works
      // against WarpStream; it originally regression-guarded a version that returned `null` for
      // `DescribeCluster.controller()`, an edge case newer stable builds may no longer exercise.
      "latest-stable"
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
