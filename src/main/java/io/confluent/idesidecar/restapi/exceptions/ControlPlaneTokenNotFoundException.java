package io.confluent.idesidecar.restapi.exceptions;

/**
 * Exception thrown when the control plane token is not found or invalid.
 */
public class ControlPlaneTokenNotFoundException extends CCloudAuthenticationFailedException {
  public ControlPlaneTokenNotFoundException(String message) {
    super(message);
  }
}