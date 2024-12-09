package io.confluent.idesidecar.scaffolding;

import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryException;
import java.util.Map;

/**
 * Interface for a rendering service that applies rendering logic to content fetched from a template
 * registry service.
 */
public interface TemplateEngine {


  /**
   * Applies a template from a provided template registry using given template options.
   *
   * @param templateRegistryService the template registry to use for looking up the template
   * @param templateName            the name of the to-be-applied template
   * @param templateOptions         the options to use for replacing placeholders in the template
   * @return the rendered files of the templates
   * @throws TemplateRegistryException if it can't read the template from the registry
   */
  Map<String, byte[]> renderTemplateFromRegistry(TemplateRegistryService templateRegistryService,
      String templateName,
      Map<String, Object> templateOptions) throws TemplateRegistryException;
}
