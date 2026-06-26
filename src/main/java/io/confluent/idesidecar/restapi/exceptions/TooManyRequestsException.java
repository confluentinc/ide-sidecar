package io.confluent.idesidecar.restapi.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Exception thrown when an upstream API returns HTTP 429 (Too Many Requests). Transient and
 * retryable; callers should retry with exponential backoff. Confluent Cloud is the in-tree
 * consumer, but the type itself is API-agnostic.
 */
@RegisterForReflection
public class TooManyRequestsException extends RuntimeException {

  private final int retryAfterSeconds;

  public TooManyRequestsException(String url, int retryAfterSeconds) {
    super("Too Many Requests (HTTP 429) from %s (retry-after: %ds)".formatted(
        url, retryAfterSeconds
    ));
    this.retryAfterSeconds = retryAfterSeconds;
  }

  public TooManyRequestsException(String url) {
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
