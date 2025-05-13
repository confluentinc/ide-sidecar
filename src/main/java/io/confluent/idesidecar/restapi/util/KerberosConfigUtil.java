package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.credentials.KerberosCredentials;
import io.confluent.idesidecar.restapi.models.Preferences.PreferencesSpec;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

@ApplicationScoped
public class KerberosConfigUtil {
  public synchronized void updateConfigPath(@Observes PreferencesSpec preferences) {
    if (preferences.kerberosConfigFilePath() != null) {
      System.setProperty(
          KerberosCredentials.KERBEROS_CONFIG_FILE_PROPERTY_NAME,
          preferences.kerberosConfigFilePath()
      );
    }
  }
}
