package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.resources.FeatureFlagsResourceTest.getFlag;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.node.BooleanNode;
import io.confluent.idesidecar.restapi.featureflags.FlagId;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class FeatureFlagsResourceIT {

  @Test
  void shouldGetFlagValues() {
    assertEquals(
        BooleanNode.getTrue(),
        getFlag(FlagId.IDE_GLOBAL_ENABLE)
    );
    assertEquals(
        BooleanNode.getTrue(),
        getFlag(FlagId.IDE_CCLOUD_ENABLE)
    );
  }
}
