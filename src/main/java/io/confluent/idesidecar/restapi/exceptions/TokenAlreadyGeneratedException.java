package io.confluent.idesidecar.restapi.exceptions;

/**
 * Exception thrown when the auth token has already been generated, i.e., the sidecar's
 * <code>/gateway/v1/handshake</code> route has already been called.
 */
public class TokenAlreadyGeneratedException extends Exception {

  public TokenAlreadyGeneratedException(String message) {
    super(message);
  }
}