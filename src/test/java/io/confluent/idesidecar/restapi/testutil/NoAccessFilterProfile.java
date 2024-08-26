package io.confluent.idesidecar.restapi.testutil;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Collections;
import java.util.Map;

public class NoAccessFilterProfile implements QuarkusTestProfile {

  /**
   * Disable the access token filter for most unit tests to not have to bother with it.
   */
  @Override
  public Map<String, String> getConfigOverrides() {
    return Collections.singletonMap("ide-sidecar.access_token_filter.enabled", "false");
  }
}