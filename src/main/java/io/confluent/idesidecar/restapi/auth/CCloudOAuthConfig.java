package io.confluent.idesidecar.restapi.auth;

import java.time.Duration;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Configuration for the {@link CCloudOAuthContext}.
 */
public final class CCloudOAuthConfig {

  public static final Duration TOKEN_REFRESH_INTERVAL_SECONDS = Duration.ofSeconds(
      ConfigProvider
          .getConfig()
          .getValue(
              "ide-sidecar.connections.ccloud.check-token-expiration.interval-seconds",
              Long.class));

  public static final Integer MAX_TOKEN_REFRESH_ATTEMPTS = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.refresh-token.max-refresh-attempts", Integer.class);

  public static final String CCLOUD_OAUTH_REDIRECT_URI = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.oauth.redirect-uri", String.class);

  public static final String CCLOUD_OAUTH_SCOPE = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.oauth.scope", String.class);

  public static final Duration CCLOUD_REFRESH_TOKEN_ABSOLUTE_LIFETIME = Duration.ofSeconds(
      ConfigProvider
          .getConfig()
          .getValue(
              "ide-sidecar.connections.ccloud.refresh-token.absolute-lifetime-seconds",
              Long.class));

  public static final Duration CCLOUD_CONTROL_PLANE_TOKEN_LIFE_TIME_SEC = Duration.ofSeconds(
      ConfigProvider
          .getConfig()
          .getValue(
              "ide-sidecar.connections.ccloud.control-plane-token.life-time-seconds",
              Long.class));

  public static final String CCLOUD_CONTROL_PLANE_TOKEN_URI = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.control-plane-token.exchange-uri", String.class);

  public static final String CCLOUD_CONTROL_PLANE_CHECK_JWT_URI = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.control-plane-token.check-jwt-uri", String.class);

  public static final String CCLOUD_DATA_PLANE_TOKEN_URI = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.data-plane-token.exchange-uri", String.class);

  public static final String CCLOUD_OAUTH_TOKEN_URI;

  public static final String CCLOUD_OAUTH_AUTHORIZE_URI;

  public static final String CCLOUD_OAUTH_CLIENT_ID;

  enum CCloudEnv {
    STAG,
    DEVEL,
    PROD;

    public static CCloudEnv of(String literal) {
      return switch (literal) {
        case "stag.cpdev.cloud" -> STAG;
        case "devel.cpdev.cloud" -> DEVEL;
        default -> PROD;
      };
    }
  }

  static {
    // Depending on the value passed via the configuration option
    // ide-sidecar.connections.ccloud.base-path, we point the sidecar to CCloud stag, devel, or
    // prod (default).
    var basePath = ConfigProvider.getConfig()
        .getValue("ide-sidecar.connections.ccloud.base-path", String.class);
    var env = CCloudEnv.of(basePath).name().toLowerCase();
    CCLOUD_OAUTH_CLIENT_ID = ConfigProvider.getConfig().getValue(
        "ide-sidecar.connections.ccloud.oauth.client-id.%s".formatted(env),
        String.class
    );
    CCLOUD_OAUTH_AUTHORIZE_URI = ConfigProvider.getConfig().getValue(
        "ide-sidecar.connections.ccloud.oauth.authorize-uri.%s".formatted(env),
        String.class
    );
    CCLOUD_OAUTH_TOKEN_URI = ConfigProvider.getConfig().getValue(
        "ide-sidecar.connections.ccloud.id-token.exchange-uri.%s".formatted(env),
        String.class
    );
  }

  private CCloudOAuthConfig() {

  }
}
