package io.confluent.idesidecar.restapi.testutil;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class FakeSideCarProfile implements QuarkusTestProfile {

  /**
   * Disable the access token filter to protest unit tests.
   */
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "vscode.version", "20.1.2",
        "os.name", "GNU/Linux",
        "os.arch", "amd64-override",
        "os.version", "5.15.0-60-generic",
        "quarkus.application.", "1.2.3",
        "vscode.extension.version", "0.21.0"
    );
  }
}
