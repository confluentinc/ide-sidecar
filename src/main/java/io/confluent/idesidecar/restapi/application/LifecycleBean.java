package io.confluent.idesidecar.restapi.application;

import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

/**
 * Lifecycle bean for the application. This bean listens for startup and shutdown events and
 * performs necessary actions.
 */
@ApplicationScoped
public class LifecycleBean {

  void onStart(@Observes StartupEvent ev) {
    Log.info("Sidecar starting...");
    // TODO: Remove
    System.setProperty("org.xerial.snappy.lib.path", "/tmp");
  }

  void onStop(@Observes ShutdownEvent ev) {
    Log.info("Sidecar stopping...");
  }

}
