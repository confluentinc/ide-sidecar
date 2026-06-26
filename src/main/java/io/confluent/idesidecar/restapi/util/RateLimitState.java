package io.confluent.idesidecar.restapi.util;

import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.ext.web.client.HttpResponse;

/**
 * Immutable snapshot of rate-limit information parsed from CCloud API response headers. Used by
 * {@link CCloudApiRateLimiter} to dynamically adjust request throughput.
 *
 * @param limit           value of {@code X-RateLimit-Limit}, or -1 if unknown
 * @param remaining       value of {@code X-RateLimit-Remaining}
 * @param resetSeconds    value of {@code X-RateLimit-Reset} (relative seconds until window resets)
 * @param receivedAtNanos {@link System#nanoTime()} when this snapshot was captured
 */
@RegisterForReflection
public record RateLimitState(
    int limit,
    int remaining,
    double resetSeconds,
    long receivedAtNanos
) {

  public static final String HEADER_LIMIT = "X-RateLimit-Limit";
  public static final String HEADER_REMAINING = "X-RateLimit-Remaining";
  public static final String HEADER_RESET = "X-RateLimit-Reset";
  public static final String HEADER_RETRY_AFTER = "Retry-After";

  /**
   * Parse rate-limit headers from an HTTP response. Returns {@code null} if any of the three
   * {@code X-RateLimit-*} headers are missing or unparseable. The all-three-absent case is silent
   * (some endpoints simply don't emit rate-limit headers); the partial-or-malformed case warns,
   * so a CCloud API change can be caught instead of silently disabling adaptive throttling.
   */
  public static RateLimitState fromHeaders(HttpResponse<?> response) {
    var limitStr = response.getHeader(HEADER_LIMIT);
    var remainingStr = response.getHeader(HEADER_REMAINING);
    var resetStr = response.getHeader(HEADER_RESET);

    if (limitStr == null || remainingStr == null || resetStr == null) {
      // silent for endpoints that simply don't emit the headers (all three null);
      // warn on partial drift so an API contract change becomes visible
      if (limitStr != null || remainingStr != null || resetStr != null) {
        Log.warnf(
            "Partial X-RateLimit-* headers from CCloud: limit=%s remaining=%s reset=%s",
            limitStr, remainingStr, resetStr
        );
      }
      return null;
    }

    try {
      return new RateLimitState(
          Integer.parseInt(limitStr.trim()),
          Integer.parseInt(remainingStr.trim()),
          Double.parseDouble(resetStr.trim()),
          System.nanoTime()
      );
    } catch (NumberFormatException e) {
      Log.warnf(
          "Unparseable X-RateLimit-* headers from CCloud: limit=%s remaining=%s reset=%s (%s)",
          limitStr, remainingStr, resetStr, e.getMessage()
      );
      return null;
    }
  }

  /**
   * Parse the {@code Retry-After} header as integer seconds. Returns -1 if absent or unparseable
   * (the header can also be an HTTP-date, which is not parsed here).
   */
  public static int parseRetryAfterHeader(HttpResponse<?> response) {
    var header = response.getHeader(HEADER_RETRY_AFTER);
    if (header != null) {
      try {
        return Integer.parseInt(header.trim());
      } catch (NumberFormatException e) {
        // Retry-After can also be an HTTP-date per RFC 7231 §7.1.3, which we don't parse.
        // Logged at DEBUG so operators can still distinguish HTTP-date from genuinely malformed
        // values if they're investigating throttling behavior.
        Log.debugf("Retry-After header is not an integer (likely an HTTP-date): %s", header);
      }
    }
    return -1;
  }
}
