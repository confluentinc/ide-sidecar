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
      // Pin an explicit agent version rather than a moving tag (`latest`/`latest-stable`): a tag
      // republish can pull a broken agent and break CI on every branch at once. v822's `playground`
      // crashes on startup (its Tableflow agent fails to load an embedded DuckDB dependency); v821
      // is the newest version that boots cleanly. This suite checks the sidecar's Internal Kafka
      // REST (ClusterV3Suite) against WarpStream; the null/empty `DescribeCluster.controller()` case
      // it originally guarded is now covered directly in ClusterManagerImplTest.
      "v821"
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
