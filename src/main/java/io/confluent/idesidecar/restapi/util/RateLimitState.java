package io.confluent.idesidecar.restapi.util;

import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.ext.web.client.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

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
   * Parse the response's {@code Retry-After} header as whole seconds from now. Returns -1 if
   * absent or unparseable.
   *
   * @see #parseRetryAfter(String, Instant)
   */
  public static int parseRetryAfterHeader(HttpResponse<?> response) {
    return parseRetryAfter(response.getHeader(HEADER_RETRY_AFTER), Instant.now());
  }

  /**
   * Parse a {@code Retry-After} value, either delay-seconds or an HTTP-date (RFC 9110 section
   * 10.2.3), as whole seconds after {@code now}. A date in the past means no wait (0). Returns -1
   * if the value is absent or unparseable.
   */
  public static int parseRetryAfter(String header, Instant now) {
    if (header == null || header.isBlank()) {
      return -1;
    }
    var value = header.trim();
    if (value.chars().allMatch(c -> c >= '0' && c <= '9')) {
      // more than 10 digits is past Integer.MAX_VALUE, and could overflow a long
      return value.length() > 10
          ? Integer.MAX_VALUE
          : (int) Math.min(Long.parseLong(value), Integer.MAX_VALUE);
    }
    try {
      var date = ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
      var millis = Duration.between(now, date).toMillis();
      return millis <= 0 ? 0 : (int) Math.min((millis + 999) / 1000, Integer.MAX_VALUE);
    } catch (DateTimeParseException e) {
      // we fall back to our own backoff
      Log.errorf(e, "Ignoring unparseable Retry-After header: %s", value);
      return -1;
    }
  }
}
