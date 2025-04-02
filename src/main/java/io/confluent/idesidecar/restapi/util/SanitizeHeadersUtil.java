package io.confluent.idesidecar.restapi.util;

import io.vertx.core.MultiMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Utility class for sanitizing HTTP headers.
 */
public final class SanitizeHeadersUtil {

  private SanitizeHeadersUtil() {
    // Private constructor to prevent instantiation
  }

  /**
   * Sanitizes HTTP headers by removing excluded headers.
   *
   * @param requestHeaders The headers to sanitize
   * @param exclusions List of header names to exclude
   * @return Map of sanitized headers
   */
  public static Map<String, String> sanitizeHeaders(MultiMap requestHeaders, List<String> exclusions) {
    var headers = new HashMap<String, String>();
    if (requestHeaders != null) {
      requestHeaders.forEach(
          entry -> headers.put(entry.getKey(), entry.getValue())
      );
    }
    if (exclusions != null) {
      exclusions.forEach(headers::remove);
    }
    return headers;
  }
}