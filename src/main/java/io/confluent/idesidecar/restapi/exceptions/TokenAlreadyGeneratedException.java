package io.confluent.idesidecar.restapi.exceptions;

/**
 * Exception thrown when the auth token has already been generated / the sidecar /handshake 
 * route has already been called.
 */
public class TokenAlreadyGeneratedException extends Exception {
  public TokenAlreadyGeneratedException(String message) {
    super(message);
  }
}