package io.confluent.idesidecar.scaffolding.exceptions;

public class TemplateNotFoundException extends TemplateRegistryException {

  public TemplateNotFoundException(String message) {
    super(message, "template_not_found");
  }

}
