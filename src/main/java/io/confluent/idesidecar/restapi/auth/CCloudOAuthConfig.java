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

  static {
    var basePath = ConfigProvider.getConfig()
        .getValue("ide-sidecar.connections.ccloud.base-path", String.class);
    switch (basePath) {
      case "stag.cpdev.cloud":
        CCLOUD_OAUTH_CLIENT_ID = "S5PWFB5AQoLRg7fmsCxtBrGhYwTTzmAu";
        CCLOUD_OAUTH_AUTHORIZE_URI = "https://login-stag.confluent-dev.io/oauth/authorize";
        CCLOUD_OAUTH_TOKEN_URI = "https://login-stag.confluent-dev.io/oauth/token";
        break;
      case "devel.cpdev.cloud":
        CCLOUD_OAUTH_CLIENT_ID = "cUmAgrkbAZSqSiy38JE7Ya3i7FwXmyUF";
        CCLOUD_OAUTH_AUTHORIZE_URI = "https://login.confluent-dev.io/oauth/authorize";
        CCLOUD_OAUTH_TOKEN_URI = "https://login.confluent-dev.io/oauth/token";
        break;
      default:
        CCLOUD_OAUTH_CLIENT_ID = "Q93zdbI3FnltpEa9G1gg6tiMuoDDBkwS";
        CCLOUD_OAUTH_AUTHORIZE_URI = "https://login.confluent.io/oauth/authorize";
        CCLOUD_OAUTH_TOKEN_URI = "https://login.confluent.io/oauth/token";
        break;
    }
  }

  private CCloudOAuthConfig() {

  }
}
