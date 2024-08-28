package io.confluent.idesidecar.scaffolding.exceptions;

/**
 * Exception thrown when an error occurs while interacting with the template registry.
 */
public class TemplateRegistryException extends Exception {

  private final String code;

  public TemplateRegistryException(String message, String code) {
    super(message);
    this.code = code;
  }

  public TemplateRegistryException(String message, String code, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public String getCode() {
    return code;
  }
}
