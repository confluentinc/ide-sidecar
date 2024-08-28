package io.confluent.idesidecar.restapi.exceptions;

/**
 * Exception thrown when a requested Sidecar connection cannot be found.
 */
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
