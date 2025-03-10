package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Utility class for working with the VS Code extension.
 */
@ApplicationScoped
public final class VsCodeExtensionUtil {

  static final String CCLOUD_OAUTH_VSCODE_EXTENSION_URI = ConfigProvider.getConfig()
      .getOptionalValue("ide-sidecar.connections.ccloud.oauth.vscode-extension-uri", String.class)
      .orElse("vscode://confluentinc.vscode-confluent/authCallback");

  /**
   * Returns the URI that the callback page should redirect to for a specific CCloud connection.
   * Attempts to read the URI from the connection's spec. If null, returns the URI set via the
   * application config <code>ide-sidecar.connections.ccloud.oauth.vscode-extension-uri</code>.
   *
   * @param connection the CCloud connection
   * @return the URI
   */
  public String getRedirectUri(CCloudConnectionState connection) {
    var connectionSpec = Optional.ofNullable(connection)
        .map(ConnectionState::getSpec)
        .orElse(null);

    if (connectionSpec != null) {
      var ccloudConfig = connectionSpec.ccloudConfig();
      if (ccloudConfig != null && ccloudConfig.ideAuthCallbackUri() != null) {
        return ccloudConfig.ideAuthCallbackUri();
      }
    }

    return CCLOUD_OAUTH_VSCODE_EXTENSION_URI;
  }
}
