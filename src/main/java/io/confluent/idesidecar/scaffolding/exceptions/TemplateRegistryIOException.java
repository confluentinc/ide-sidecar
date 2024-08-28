package io.confluent.idesidecar.scaffolding.exceptions;

/**
 * Exception thrown when an IO error occurs while interacting with the template registry.
 */
public class TemplateRegistryIOException extends RuntimeException {

  private final String code;

  public TemplateRegistryIOException(String message, String code) {
    super(message);
    this.code = code;
  }

  public TemplateRegistryIOException(String message, String code, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public String getCode() {
    return code;
  }
}
