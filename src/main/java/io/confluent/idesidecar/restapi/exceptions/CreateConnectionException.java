package io.confluent.idesidecar.restapi.exceptions;


public class CreateConnectionException extends Exception {

  public CreateConnectionException() {
    super("Error while creating connection.");
  }

  public CreateConnectionException(String message) {
    super(message);
  }

  public CreateConnectionException(String message, Throwable cause) {
    super(message, cause);
  }

  public CreateConnectionException(Throwable cause) {
    super(cause);
  }
}