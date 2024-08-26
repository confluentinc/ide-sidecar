package io.confluent.idesidecar.restapi.auth;

import java.time.Duration;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Configuration for the <i>CCloudOAuthContext</i>.
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

  public static final String CCLOUD_OAUTH_TOKEN_URI = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.id-token.exchange-uri", String.class);

  public static final String CCLOUD_OAUTH_CLIENT_ID = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.oauth.client-id", String.class);

  public static final String CCLOUD_OAUTH_AUTHORIZE_URI = ConfigProvider.getConfig()
      .getValue("ide-sidecar.connections.ccloud.oauth.authorize-uri", String.class);

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

  private CCloudOAuthConfig() {

  }
}
