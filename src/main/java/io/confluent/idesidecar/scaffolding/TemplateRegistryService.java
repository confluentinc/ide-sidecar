package io.confluent.idesidecar.scaffolding;

import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryException;
import io.confluent.idesidecar.scaffolding.models.TemplateManifest;
import java.util.List;
import java.util.Map;

/**
 * TemplateRegistry is an interface that defines the contract for a template registry.
 */
public interface TemplateRegistryService {

  /**
   * Hook for any startup logic that needs to be executed before the registry can be queried for
   * templates.
   */
  default void onStartup() throws TemplateRegistryException {
    // Do nothing by default
  }

  /**
   * Hook for any shutdown logic that needs to be executed before the registry is destroyed.
   */
  default void onShutdown() throws TemplateRegistryException {
    // Do nothing by default
  }

  /**
   * List all templates in the registry.
   */
  List<TemplateManifest> listTemplates();

  /**
   * Get a specific template from the registry.
   */
  TemplateManifest getTemplate(String templateName) throws TemplateRegistryException;

  /**
   * Get the source contents of the template as a map of file names to file contents.
   */
  Map<String, byte[]> getTemplateSrcContents(String templateName);

  /**
   * Get the static contents of the template as a map of file names to file contents. These static
   * files must not be rendered but should be copied as-is.
   */
  Map<String, byte[]> getTemplateStaticContents(String templateName);
}
