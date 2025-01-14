package io.confluent.idesidecar.restapi.exceptions;

import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import java.util.List;

/**
 * Exception thrown when the provided input/request payload is not valid.
 */
public class InvalidInputException extends RuntimeException {

  private final List<Error> errors;

  public InvalidInputException(List<Error> errors) {
    super("Invalid input");
    this.errors = errors;
  }

  public InvalidInputException(Error error) {
    super("Invalid input");
    this.errors = List.of(error);
  }

  public List<Error> errors() {
    return errors;
  }
}
