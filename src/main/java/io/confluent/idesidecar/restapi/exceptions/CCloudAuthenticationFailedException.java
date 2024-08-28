package io.confluent.idesidecar.restapi.exceptions;

/**
 * Exception thrown when an error occurs during authentication with the Confluent Cloud API.
 */
public class CCloudAuthenticationFailedException extends RuntimeException {

  public CCloudAuthenticationFailedException(String message) {
    super(message);
  }

  public CCloudAuthenticationFailedException(String message, Throwable cause) {
    super(message, cause);
  }

}
