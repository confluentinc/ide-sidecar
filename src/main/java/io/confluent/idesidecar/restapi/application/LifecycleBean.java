package io.confluent.idesidecar.restapi.application;


import io.confluent.idesidecar.scaffolding.TemplateRegistryService;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

/**
 * Lifecycle bean for the application. This bean listens for startup and shutdown events and
 * performs necessary actions. In this case, it clones or updates the templates from the default
 * template registry. If the operation fails, the application will log an error and continue.
 */
@ApplicationScoped
public class LifecycleBean {

  @Inject
  TemplateRegistryService defaultTemplateRegistryService;

  void onStart(@Observes StartupEvent ev) {
    Log.info("Sidecar starting...");

    try {
      defaultTemplateRegistryService.onStartup();
    } catch (TemplateRegistryException e) {
      // Don't throw exception here because the sidecar application should still start even
      // if we fail to initialize the default template registry.
      Log.error("Failed to initialize default template registry", e);
    }
  }

  void onStop(@Observes ShutdownEvent ev) throws TemplateRegistryException {
    Log.info("Sidecar stopping...");

    defaultTemplateRegistryService.onShutdown();
  }

}
