package io.confluent.idesidecar.restapi.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.ws.rs.core.Response;

/**
 * Exception thrown when there is an error in the template generation process.
 */
@RegisterForReflection
public class ScaffoldingException extends RuntimeException {

  private final Failure failure;

  public ScaffoldingException(String message, Failure failure, Throwable t) {
    super(message, t);
    this.failure = failure;
  }

  public ScaffoldingException(Failure failure) {
    super(failure.title());
    this.failure = failure;
  }

  public ScaffoldingException(Throwable t) {
    super(t);
    this.failure = new Failure(
        (Exception) t,
        Response.Status.INTERNAL_SERVER_ERROR,
        "internal_error",
        "An internal error occurred",
        null,
        null
    );
  }

  public Failure getFailure() {
    return failure;
  }
}

