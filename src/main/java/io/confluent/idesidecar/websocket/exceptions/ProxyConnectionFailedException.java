package io.confluent.idesidecar.websocket.exceptions;

public class ProxyConnectionFailedException extends RuntimeException {
  public ProxyConnectionFailedException(Exception cause) {
    super("Failed to connect to the proxy server", cause);
  }
}
