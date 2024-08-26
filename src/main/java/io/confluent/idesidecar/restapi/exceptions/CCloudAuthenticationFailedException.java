package io.confluent.idesidecar.restapi.exceptions;

public class CCloudAuthenticationFailedException extends RuntimeException {

  public CCloudAuthenticationFailedException(String message) {
    super(message);
  }

  public CCloudAuthenticationFailedException(String message, Throwable cause) {
    super(message, cause);
  }

}
