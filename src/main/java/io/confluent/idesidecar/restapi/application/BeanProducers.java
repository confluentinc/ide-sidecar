package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.scaffolding.LocalTemplateRegistryService;
import io.confluent.idesidecar.scaffolding.MustacheTemplateEngine;
import io.confluent.idesidecar.scaffolding.TemplateEngine;
import io.confluent.idesidecar.scaffolding.TemplateRegistryService;
import io.confluent.idesidecar.scaffolding.util.ZipUtil;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.io.IOException;
import java.util.Objects;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Defines <a
 * href="https://quarkus.io/guides/cdi-reference#simplified-producer-method-declaration">producer
 * methods</a> that set up beans for dependency injection.
 */
public class BeanProducers {

  @Produces
  @ApplicationScoped
  public TemplateRegistryService localTemplateRegistryService(
      @ConfigProperty(name = "ide-sidecar.template-registries.local.templates-zip-resource")
      String templatesZipResource
  ) {
    try {
      var templatesZipContents = Objects.requireNonNull(
              Thread.currentThread().getContextClassLoader().getResourceAsStream(
                  templatesZipResource))
          .readAllBytes();
      var tempDir = ZipUtil.extractZipContentsToTempDir(templatesZipContents);
      Log.info("Extracted templates to %s".formatted(tempDir));
      return new LocalTemplateRegistryService(tempDir);
    } catch (IOException e) {
      Log.error("Failed to extract templates from the ZIP resource. "
          + "Creating an empty registry instead.", e);
      return new LocalTemplateRegistryService(null);
    }
  }

  @Produces
  @ApplicationScoped
  public TemplateEngine templateEngine() {
    return new MustacheTemplateEngine();
  }
}
