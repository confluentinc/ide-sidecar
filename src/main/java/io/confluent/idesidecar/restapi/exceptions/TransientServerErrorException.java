package io.confluent.idesidecar.restapi.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.Objects;

/**
 * Thrown when an upstream API answers 502, 503 or 504, which a retry may get past. Carries the
 * status and {@code Retry-After}, and the error the response would otherwise have produced as
 * its cause, so a caller that gives up can report that error unchanged.
 */
@RegisterForReflection
public class TransientServerErrorException extends RuntimeException {

  private final int status;
  private final int retryAfterSeconds;

  /**
   * Create one for a {@code status} response from {@code url}.
   *
   * @param retryAfterSeconds the response's {@code Retry-After}, or -1 if it had none
   * @param error             the error the response would otherwise have produced
   */
  public TransientServerErrorException(
      String url,
      int status,
      int retryAfterSeconds,
      RuntimeException error
  ) {
    super(
        "HTTP %d from %s (retry-after: %ds)".formatted(status, url, retryAfterSeconds),
        Objects.requireNonNull(error)
    );
    this.status = status;
    this.retryAfterSeconds = retryAfterSeconds;
  }

  public int getStatus() {
    return status;
  }

  /**
   * The response's {@code Retry-After} in seconds, or -1 if it had none.
   */
  public int getRetryAfterSeconds() {
    return retryAfterSeconds;
  }
}
