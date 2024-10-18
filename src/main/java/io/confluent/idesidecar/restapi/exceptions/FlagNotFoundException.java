package io.confluent.idesidecar.restapi.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Exception thrown when a requested flag cannot be found.
 */
@RegisterForReflection
public class FlagNotFoundException extends RuntimeException {

  public FlagNotFoundException(String id) {
    super(
        "Flag '%s' not found.".formatted(id)
    );
  }

  public FlagNotFoundException(String id, Throwable cause) {
    super(
        "Flag '%s' not found.".formatted(id),
        cause
    );
  }

  public FlagNotFoundException(Throwable cause) {
    super(cause);
  }
}
