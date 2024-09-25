package io.confluent.idesidecar.restapi.exceptions;

import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import java.util.List;

/**
 * Exception thrown when the provided preferences are not valid.
 */
public class InvalidPreferencesException extends Exception {

  private final List<Error> errors;

  public InvalidPreferencesException(List<Error> errors) {
    super("Invalid preferences");
    this.errors = errors;
  }

  public List<Error> errors() {
    return errors;
  }
}
