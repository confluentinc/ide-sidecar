/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.kafkarest.api;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class PartitionV3ApiImplDirectIT extends PartitionV3ApiImplIT {

  @BeforeEach
  public void beforeEach() {
    setupDirectConnection(this);
  }

}
