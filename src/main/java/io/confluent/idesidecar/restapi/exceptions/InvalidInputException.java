package io.confluent.idesidecar.restapi.exceptions;

import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import java.util.List;

public class InvalidInputException extends RuntimeException {

  private final List<Error> errors;

  public InvalidInputException(List<Error> errors) {
    super("Invalid input");
    this.errors = errors;
  }

  public List<Error> errors() {
    return errors;
  }
}
