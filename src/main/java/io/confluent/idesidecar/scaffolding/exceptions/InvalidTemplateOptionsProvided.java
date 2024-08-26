package io.confluent.idesidecar.scaffolding.exceptions;

import io.confluent.idesidecar.scaffolding.models.TemplateManifest.Error;
import java.util.List;

public class InvalidTemplateOptionsProvided extends TemplateRegistryException {

  private final List<Error> errors;

  public InvalidTemplateOptionsProvided(List<Error> errors) {
    super("Invalid template options provided", "invalid_template_options");
    this.errors = errors;
  }

  public List<Error> getErrors() {
    return errors;
  }

}
