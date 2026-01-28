package io.confluent.idesidecar.restapi.util;

import java.util.HashMap;
import java.util.Map;

public final class SchemaRegistryUtil {

  private SchemaRegistryUtil() {

  }

  /**
   * Remove OAuth-related configuration properties from the given Schema Registry configuration map.
   *
   * @param configProperties The original Schema Registry configuration properties.
   * @return A new map with OAuth configuration properties removed.
   */
  public static Map<String, Object> removeOAuthConfigs(Map<String, Object> configProperties) {
    var filteredConfig = new HashMap<String, Object>();
    for (var configProperty : configProperties.entrySet()) {
      String key = configProperty.getKey();
      if (!key.startsWith("bearer.auth.")) {
        filteredConfig.put(key, configProperty.getValue());
      }
    }
    return filteredConfig;
  }
}
