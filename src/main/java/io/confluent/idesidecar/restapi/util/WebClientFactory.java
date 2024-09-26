package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.models.Preferences;
import io.confluent.idesidecar.restapi.models.Preferences.PreferencesSpec;
import io.quarkus.arc.Arc;
import io.quarkus.logging.Log;
import io.vertx.core.Vertx;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

/**
 * Factory class for {@link WebClient}s. Returns a new Vert.x web client while making sure that a
 * single Vert.x instance is used for the entire application.
 */
@ApplicationScoped
public class WebClientFactory {

  /**
   * It's important that we use the Quarkus-managed Vertx instance here
   * and not create a new Vertx instance ourselves, as this would not pick up the
   * Quarkus configuration to disable caching {@code quarkus.vertx.cache = false}.
   * We need to disable caching since Vertx running in the native executable tries to look up
   * the cache directory tmp path of the machine it was built on, which breaks when the
   * executable is run on any other machine.
   */
  @Inject
  protected Vertx vertx;

  /**
   * We don't declare this as final and initialize this in a constructor because then we'd
   * have to dependency inject the Vertx instance as a constructor arg,
   * which is not possible given that we instantiate this class outside DI contexts.
   * Hence, we lazily initialize this instance in {@link #getWebClient()}
   * and manage it as a singleton.
   */
  private WebClient webClient;

  public synchronized WebClient getWebClient() {
    if (webClient == null) {
      webClient = WebClient.create(getVertx());
    }

    return webClient;
  }

  /**
   * Observer method that is called whenever the {@link Preferences} are updated. Updates the
   * configuration of the web client according to the passed {@link Preferences} instance.
   *
   * @param preferences The new {@link Preferences}
   */
  public synchronized void updateWebClientOptions(@Observes PreferencesSpec preferences) {
    var clientOptions = new WebClientOptions();

    clientOptions.setTrustAll(Boolean.TRUE.equals(preferences.trustAllCertificates()));

    if (preferences.tlsPemPaths() != null) {
      var pemTrustOptions = new PemTrustOptions();
      for (var pemPath : preferences.tlsPemPaths()) {
        pemTrustOptions.addCertPath(pemPath);
      }
      clientOptions.setPemTrustOptions(pemTrustOptions);
    }

    webClient = WebClient.create(getVertx(), clientOptions);

    Log.debugf("Updated the Vert.x web client config to: %s", clientOptions);
  }

  private Vertx getVertx() {
    // Will be null when not injected via CDI.
    if (vertx == null) {
      // Look up the Vertx instance from the CDI container.
      vertx = Arc.container().select(Vertx.class).get();
    }

    return vertx;
  }
}
