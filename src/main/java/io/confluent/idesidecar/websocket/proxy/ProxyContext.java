package io.confluent.idesidecar.websocket.proxy;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.util.FlinkPrivateEndpointUtil;
import jakarta.websocket.Session;
import jakarta.enterprise.inject.spi.CDI;
import org.eclipse.microprofile.config.ConfigProvider;
import java.util.List;
import java.util.Optional;

public record ProxyContext (
    String connectionId,
    String region,
    String provider,
    String environmentId,
    String organizationId,
    CCloudConnectionState connection
) {

  static final String LANGUAGE_SERVICE_URL_PATTERN = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.flink-language-service-proxy.url-pattern", String.class);
  static final String LANGUAGE_SERVICE_PRIVATE_URL_PATTERN = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.flink-language-service-proxy.private-url-pattern", String.class);

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
        paramMap.get(ORGANIZATION_ID_PARAM_NAME).get(0),
        null
    );
  }

  public ProxyContext withConnection(CCloudConnectionState connection) {
    return new ProxyContext(
        connectionId,
        region,
        provider,
        environmentId,
        organizationId,
        connection
    );
  }

  public String getConnectUrl() {
    String urlPattern = getUrlPattern();
    return urlPattern
        .replace("{{ region }}", region)
        .replace("{{ provider }}", provider);
  }

  private String getUrlPattern() {
    return hasPrivateEndpoints() ?
        LANGUAGE_SERVICE_PRIVATE_URL_PATTERN :
        LANGUAGE_SERVICE_URL_PATTERN;
  }

  private boolean hasPrivateEndpoints() {
    FlinkPrivateEndpointUtil util = CDI.current()
        .select(FlinkPrivateEndpointUtil.class)
        .get();
    List<String> endpoints = util.getPrivateEndpoints(environmentId);
    return endpoints != null && !endpoints.isEmpty();
  }
}
