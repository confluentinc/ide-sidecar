package io.confluent.idesidecar.scaffolding.exceptions;

/**
 * Exception thrown when the requested template cannot be found.
 */
public class TemplateNotFoundException extends TemplateRegistryException {

  public TemplateNotFoundException(String message) {
    super(message, "template_not_found");
  }

}
