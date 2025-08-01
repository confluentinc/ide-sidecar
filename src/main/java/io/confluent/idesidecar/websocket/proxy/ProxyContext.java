package io.confluent.idesidecar.websocket.proxy;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.util.FlinkPrivateEndpointUtil;
import io.quarkus.logging.Log;
import jakarta.websocket.Session;
import jakarta.enterprise.inject.spi.CDI;
import org.eclipse.microprofile.config.ConfigProvider;
import java.net.URI;
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

  static final String CONNECTION_ID_PARAM_NAME = "connectionId";
  static final String REGION_PARAM_NAME = "region";
  static final String PROVIDER_PARAM_NAME = "provider";
  static final String ENVIRONMENT_ID_PARAM_NAME = "environmentId";
  static final String ORGANIZATION_ID_PARAM_NAME = "organizationId";
  static final String FLINK_PREFIX = "flink.";
  static final String LANGUAGE_SERVICE_PREFIX = "flinkpls.";
  static final String WSS_SCHEME = "wss";
  static final String LSP_PATH = "/lsp";

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
    Optional<String> privateEndpoint = getMatchingPrivateEndpoint();
    if (privateEndpoint.isPresent()) {
        return transformToLanguageServiceUrl(privateEndpoint.get());
    }
    return buildPublicUrl();
  }

  private Optional<String> getMatchingPrivateEndpoint() {
    FlinkPrivateEndpointUtil util = getPrivateEndpointUtil();
    List<String> endpoints = util.getPrivateEndpoints(environmentId);

    if (endpoints == null || endpoints.isEmpty()) {
      return Optional.empty();
    }

    return endpoints.stream()
        .filter(endpoint ->
            util.isValidEndpointWithMatchingRegionAndProvider(endpoint, region, provider))
        .findFirst();
  }

  private FlinkPrivateEndpointUtil getPrivateEndpointUtil() {
    return CDI.current()
        .select(FlinkPrivateEndpointUtil.class)
        .get();
  }

  /**
   * Transform a Flink private endpoint URL to a Language Service URL.
   * Example: https://flink.us-west-2.aws.private.confluent.cloud
   *       -> wss://flinkpls.us-west-2.aws.private.confluent.cloud/lsp
   */
  private String transformToLanguageServiceUrl(String privateEndpoint) {
    try {
      URI uri = URI.create(privateEndpoint);
      String languageServiceHost = uri.getHost().replace(FLINK_PREFIX, LANGUAGE_SERVICE_PREFIX);

      return String.format("%s://%s%s", WSS_SCHEME, languageServiceHost, LSP_PATH);
    } catch (IllegalArgumentException e) {
      Log.debugf("Malformed private endpoint: %s", privateEndpoint);
    } catch (NullPointerException e) {
      Log.debugf("Null private endpoint: %s", privateEndpoint);
    } catch (Exception e) {
      Log.debugf("Error transforming private endpoint '%s': %s", privateEndpoint, e.getMessage());
    }

    return buildPublicUrl();
  }

  private String buildPublicUrl() {
    return LANGUAGE_SERVICE_URL_PATTERN
        .replace("{{ region }}", region)
        .replace("{{ provider }}", provider);
  }
}
