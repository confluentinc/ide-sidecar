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
        "vscode.version", "0.4.0 override",
        "os.name", "linux-override",
        "os.arch", "amd64-override",
        "os.version", "5.11.0-27-azure-override",
        "version", "0.105.0 override",
        "vscode.extension.version", "0.21.3 override"
    );
  }
}
