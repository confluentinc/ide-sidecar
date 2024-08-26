package io.confluent.idesidecar.restapi.exceptions;

public class ConnectionNotFoundException extends RuntimeException {

  public ConnectionNotFoundException() {
    super("Connection not found.");
  }

  public ConnectionNotFoundException(String message) {
    super(message);
  }

  public ConnectionNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectionNotFoundException(Throwable cause) {
    super(cause);
  }
}
