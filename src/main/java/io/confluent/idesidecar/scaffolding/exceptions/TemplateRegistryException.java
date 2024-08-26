package io.confluent.idesidecar.scaffolding.exceptions;

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
