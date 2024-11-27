package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.models.Preferences;
import io.confluent.idesidecar.restapi.models.Preferences.PreferencesSpec;
import io.quarkus.arc.Arc;
import io.quarkus.logging.Log;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Factory class for {@link WebClient}s. Returns a new Vert.x web client while making sure that a
 * single Vert.x instance is used for the entire application.
 */
@ApplicationScoped
public class WebClientFactory {

  static final String LINE_SEPARATOR = System.lineSeparator();
  static final String CERT_HEADER = "-----BEGIN CERTIFICATE-----" + LINE_SEPARATOR;
  static final String CERT_FOOTER = LINE_SEPARATOR + "-----END CERTIFICATE-----" + LINE_SEPARATOR;
  static final List<String> WINDOWS_TRUST_STORE_NAMES = List.of("WINDOWS-MY", "WINDOWS-ROOT");

  static final Duration WEBCLIENT_CONNECT_TIMEOUT_SECONDS = Duration.ofSeconds(
      ConfigProvider
          .getConfig()
          .getValue(
              "ide-sidecar.webclient.connect-timeout-seconds",
              Long.class));

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
      webClient = WebClient.create(getVertx(), getDefaultWebClientOptions());
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
    var clientOptions = getDefaultWebClientOptions();

    if (Boolean.TRUE.equals(preferences.trustAllCertificates())) {
      clientOptions.setTrustAll(true);
    }

    var tlsPemPaths = preferences.tlsPemPaths();
    if (tlsPemPaths != null && !tlsPemPaths.isEmpty()) {
      var pemTrustOptions = (PemTrustOptions) clientOptions.getSslOptions().getTrustOptions();
      if (pemTrustOptions == null) {
        pemTrustOptions = new PemTrustOptions();
      }
      // Make sure that we don't lose access to certs baked into the native executable. On Windows,
      // we perform this action when calling WebClientFactory#getDefaultWebClientOptions(), allowing
      // us to skip it here.
      if (OperatingSystemType.current() != OperatingSystemType.Windows) {
        addCertsFromBuiltInTrustStore(pemTrustOptions);
      }
      for (var pemPath : tlsPemPaths) {
        pemTrustOptions.addCertPath(pemPath);
      }
      clientOptions.setPemTrustOptions(pemTrustOptions);
    }

    webClient = WebClient.create(getVertx(), clientOptions);

    Log.debugf("Updated the Vert.x web client config to: %s", clientOptions);
  }

  WebClientOptions getDefaultWebClientOptions() {
    var clientOptions = new WebClientOptions();
    clientOptions.setConnectTimeout((int) WEBCLIENT_CONNECT_TIMEOUT_SECONDS.toMillis());
    if (OperatingSystemType.current() == OperatingSystemType.Windows) {
      var pemTrustOptions = new PemTrustOptions();
      addCertsFromBuiltInTrustStore(pemTrustOptions);
      for (var trustStoreName : WINDOWS_TRUST_STORE_NAMES) {
        addCertsFromSystemKeyStore(trustStoreName, pemTrustOptions);
      }
      // We should not update the WebClient's PEM trust options if we haven't been able to read
      // certs from the baked-in or system trust store.
      if (!pemTrustOptions.getCertValues().isEmpty()) {
        clientOptions.setPemTrustOptions(pemTrustOptions);
      }
    }

    return clientOptions;
  }

  private Vertx getVertx() {
    // Will be null when not injected via CDI.
    if (vertx == null) {
      // Look up the Vertx instance from the CDI container.
      vertx = Arc.container().select(Vertx.class).get();
    }

    return vertx;
  }

  /**
   * Add all certificates from built-in trust store to a given {@link PemTrustOptions} object.
   * @param pemTrustOptions The {@link PemTrustOptions} object to add the certificates to.
   */
  void addCertsFromBuiltInTrustStore(PemTrustOptions pemTrustOptions) {
    try {
      // Load certs from trust store baked into native executable so that we don't lose access to
      // them
      Log.info("Loading certificates from build-time trust store.");
      var trustManagerFactory = TrustManagerFactory
          .getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init((KeyStore) null);
      for (var trustManager : trustManagerFactory.getTrustManagers()) {
        if (trustManager instanceof X509TrustManager x509TrustManager) {
          for (var cert : x509TrustManager.getAcceptedIssuers()) {
            pemTrustOptions.addCertValue(Buffer.buffer(certToString(cert)));
          }
        }
      }
    } catch (NoSuchAlgorithmException | KeyStoreException | CertificateEncodingException e) {
      var stringWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stringWriter));
      Log.errorf(
          "Error loading cert from built-in trust store: %s - %s %s",
          e.getClass(),
          e.getMessage(),
          stringWriter
      );
    }
  }

  /**
   * Add all certificates from a given key store to a given {@link PemTrustOptions} object.
   *
   * @param keyStoreType The key store type to read the certificates from.
   * @param pemTrustOptions The {@link PemTrustOptions} object to add the certificates to.
   */
  void addCertsFromSystemKeyStore(String keyStoreType, PemTrustOptions pemTrustOptions) {
    try {
      // Load certs from provided trust store
      Log.infof("Loading certificates from system key store %s.", keyStoreType);
      var keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(null, null);
      var countByType = new HashMap<String, Integer>();
      var it = keyStore.aliases().asIterator();
      while (it.hasNext()) {
        var certAlias = it.next();
        var cert = keyStore.getCertificate(certAlias);
        var type = cert.getClass().getCanonicalName();
        Log.debugf("Adding certificate %s of type %s", certAlias, type);
        pemTrustOptions.addCertValue(Buffer.buffer(certToString(cert)));
        countByType.compute(type, (k, v) -> v == null ? 1 : v + 1);
      }
      // Log the number of certificates added for each type
      countByType.forEach((type, count) ->
          Log.infof("Added %d certificates from %s of type %s", count, keyStoreType, type)
      );
    } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
      var stringWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stringWriter));
      Log.errorf(
          "Error loading cert from system key store: %s - %s %s",
          e.getClass(),
          e.getMessage(),
          stringWriter
      );
    }
  }

  /**
   * Convert a {@link Certificate} to the PEM format.
   *
   * @param cert The {@link Certificate}
   * @return The certificate in the PEM format
   * @throws CertificateEncodingException when failing to encode the {@link Certificate}
   */
  String certToString(Certificate cert) throws CertificateEncodingException {
    var sw = new StringWriter();
    sw.write(CERT_HEADER);
    sw.write(
        DatatypeConverter
            .printBase64Binary(cert.getEncoded())
            .replaceAll("(.{64})", "$1" + LINE_SEPARATOR)
    );
    sw.write(CERT_FOOTER);
    return sw.toString();
  }
}
