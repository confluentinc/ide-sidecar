package io.confluent.idesidecar.restapi.util;

import java.util.HashMap;
import java.util.Map;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Utility class for extracting configuration properties.
 */
public final class ConfigUtil {
  private ConfigUtil() {
  }

  /**
   * Used to extract a map of properties from the configuration that start with the given prefix.
   * Usage: If the configuration contains the properties (as YAML):
   * <pre>
   *   ide-sidecar.serde-configs:
   *     key1: value1
   *     key2: value2
   *     "key.with.dots": value3
   *  </pre>
   *  Then you'd call {@code ConfigUtil.asMap("ide-sidecar.serde-configs")} to get a map of
   *  the properties under the prefix {@code ide-sidecar.serde-configs}.
   * @param prefixWithoutDot The prefix to search for in the configuration.
   * @return                 A map of properties that start with the given prefix.
   */
  public static Map<String, String> asMap(String prefixWithoutDot) {
    if (prefixWithoutDot.endsWith(".")) {
      throw new IllegalArgumentException(
          "Prefix should not end with a dot. Let us do the dotting. Thanks."
      );
    }
    var prefix = prefixWithoutDot + ".";
    Map<String, String> map = new HashMap<>();
    var config = ConfigProvider.getConfig();
    for (String key : config.getPropertyNames()) {
      if (key.startsWith(prefix)) {
        String mapKey = key.substring(prefix.length());
        String mapValue = config.getValue(key, String.class);
        map.put(
            removeSurroundingQuotes(mapKey),
            removeSurroundingQuotes(mapValue)
        );
      }
    }
    return map;
  }

  private static String removeSurroundingQuotes(String s) {
    if (s.startsWith("\"") && s.endsWith("\"")) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }
}
