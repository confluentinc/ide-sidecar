package io.confluent.idesidecar.restapi.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Exception thrown when a CCloud API returns HTTP 429 (Too Many Requests). This is a transient,
 * retryable condition; callers should retry with exponential backoff.
 */
@RegisterForReflection
public class CCloudRateLimitException extends RuntimeException {

  private final int retryAfterSeconds;

  public CCloudRateLimitException(String url, int retryAfterSeconds) {
    super("CCloud API rate limit exceeded for %s (retry-after: %ds)".formatted(
        url, retryAfterSeconds
    ));
    this.retryAfterSeconds = retryAfterSeconds;
  }

  public CCloudRateLimitException(String url) {
    this(url, -1);
  }

  /**
   * Returns the value of the Retry-After header from the 429 response, or -1 if the header was
   * not present.
   */
  public int getRetryAfterSeconds() {
    return retryAfterSeconds;
  }
}
