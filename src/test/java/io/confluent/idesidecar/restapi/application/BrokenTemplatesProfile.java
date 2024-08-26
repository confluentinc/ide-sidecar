package io.confluent.idesidecar.restapi.application;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Collections;
import java.util.Map;

public class BrokenTemplatesProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    return Collections.singletonMap(
        "ide-sidecar.template-registries.local.templates-zip-resource",
        "static/broken-templates.zip"
    );
  }
}
