package io.confluent.idesidecar.restapi.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.auth.CCloudOAuthConfig.CCloudEnv;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class CCloudOAuthConfigTest {

  @Test
  void cCloudEnvShouldBeCorrectlyParsedFromBasePath() {
    // Should correctly parse prod environment
    assertEquals(CCloudEnv.PROD, CCloudEnv.of("confluent.cloud"));
    // Should correctly parse stag environment
    assertEquals(CCloudEnv.STAG, CCloudEnv.of("stag.cpdev.cloud"));
    // Should correctly parse devel environment
    assertEquals(CCloudEnv.DEVEL, CCloudEnv.of("devel.cpdev.cloud"));
    // Should return prod environment by default
    assertEquals(CCloudEnv.PROD, CCloudEnv.of(""));
  }
}
