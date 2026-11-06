package io.confluent.idesidecar.restapi.clients;

import io.confluent.idesidecar.restapi.util.TimeUtil;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.time.Duration;
import java.util.regex.Pattern;

/**
 * Thrown by {@link SidecarRestService} when Schema Registry answered 429 or 503 with a
 * {@code Retry-After} that we could not honor: the wait would pass the caller's deadline or the
 * longest allowed wait, or the retries ran out. Keeps the server's status, error code, and
 * message. The server's error is attached as suppressed rather than as the cause, because
 * {@code ExceptionUtil.unwrap} follows causes and would turn this back into a plain 429.
 */
@RegisterForReflection
public class RetryAfterException extends RestClientException {

  private static final Pattern ERROR_CODE_SUFFIX = Pattern.compile("; error code: -?\\d+$");

  private final Duration retryAfter;

  /**
   * Wrap {@code serverError}, saying why its {@code retryAfter} wasn't honored.
   */
  public RetryAfterException(RestClientException serverError, Duration retryAfter, String reason) {
    super(
        "Schema Registry asked to retry after %s, but %s: %s".formatted(
            describe(retryAfter),
            reason,
            // the superclass appends the error code again
            ERROR_CODE_SUFFIX.matcher(serverError.getMessage()).replaceFirst("")
        ),
        serverError.getStatus(),
        serverError.getErrorCode()
    );
    this.retryAfter = retryAfter;
    addSuppressed(serverError);
  }

  /**
   * Format {@code duration} like other schema-fetch errors, showing a zero or negative one as
   * {@code 0s}.
   */
  static String describe(Duration duration) {
    return duration.isNegative() || duration.isZero()
        ? "0s"
        : TimeUtil.humanReadableDuration(duration);
  }

  /**
   * The wait the server asked for in its {@code Retry-After} header.
   */
  public Duration getRetryAfter() {
    return retryAfter;
  }
}
