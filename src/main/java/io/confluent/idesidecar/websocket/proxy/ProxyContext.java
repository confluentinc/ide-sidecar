package io.confluent.idesidecar.websocket.proxy;

import jakarta.websocket.Session;
import org.eclipse.microprofile.config.ConfigProvider;

public record ProxyContext (
    String connectionId,
    String region,
    String provider,
    String environmentId,
    String organizationId
) {

  static final String LANGUAGE_SERVICE_URL_PATTERN = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.flink-language-service-proxy.url-pattern", String.class);

  static final String CONNECTION_ID_PARAM_NAME = "connectionId";
  static final String REGION_PARAM_NAME = "region";
  static final String PROVIDER_PARAM_NAME = "provider";
  static final String ENVIRONMENT_ID_PARAM_NAME = "environmentId";
  static final String ORGANIZATION_ID_PARAM_NAME = "organizationId";

  public static ProxyContext from(Session session) {
    var paramMap = session.getRequestParameterMap();
    return new ProxyContext(
        paramMap.get(CONNECTION_ID_PARAM_NAME).get(0),
        paramMap.get(REGION_PARAM_NAME).get(0),
        paramMap.get(PROVIDER_PARAM_NAME).get(0),
        paramMap.get(ENVIRONMENT_ID_PARAM_NAME).get(0),
        paramMap.get(ORGANIZATION_ID_PARAM_NAME).get(0)
    );
  }

  public String getConnectUrl() {
    // TODO: I guess this won't work for private networks, we'll need something more sophisticated
    return LANGUAGE_SERVICE_URL_PATTERN
        .replace("{{ region }}", region)
        .replace("{{ provider }}", provider);
  }
}
