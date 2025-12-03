package io.confluent.idesidecar.restapi.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Ticker;
import io.confluent.idesidecar.restapi.credentials.OAuthCredentials;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth.OauthCredentialProvider;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A Schema Registry Client that implements our custom SchemaRegistryClient interface.
 */
public class SidecarSchemaRegistryClient
    extends io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
    implements SchemaRegistryClient {

  private final RestService restService;

  public SidecarSchemaRegistryClient(
      RestService restService,
      int cacheCapacity,
      List<SchemaProvider> providers,
      Map<String, ?> configs,
      Map<String, String> httpHeaders,
      Map<String, Object> clientConfig
  ) {
    super(restService, cacheCapacity, providers, configs, httpHeaders, Ticker.systemTicker());
    this.restService = restService;
    maybeConfigureOAuthCredentials(clientConfig);
  }

  public SidecarSchemaRegistryClient(
      RestService restService,
      int cacheCapacity,
      Map<String, Object> clientConfig
  ) {
    super(restService, cacheCapacity);
    this.restService = restService;
    maybeConfigureOAuthCredentials(clientConfig);
  }

  public <T> T httpRequest(String path,
      String method,
      byte[] requestBodyData,
      Map<String, String> requestProperties,
      TypeReference<T> responseFormat)
      throws IOException, RestClientException {
    return restService.httpRequest(
        path,
        method,
        requestBodyData,
        requestProperties,
        responseFormat
    );
  }

  /**
   * SchemaRegistry's BearerAuthCredentialProviderFactory makes use of dynamic service loading,
   * which does not seem to work with all GraalVM versions and might cause issues, like
   * https://github.com/confluentinc/vscode/issues/2647, so we want to manually configure
   * the OAuthCredentialProvider if the user chose to authenticate with the Schema Registry
   * using OAuth.
   *
   * @param config The configuration of the Schema Registry Client.
   */
  private void maybeConfigureOAuthCredentials(Map<String, Object> config) {
    if (
        OAuthCredentials.OAUTHBEARER_CREDENTIALS_SOURCE.equals(
            config.get(SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE)
        )
    ) {
      var credentialProvider = new OauthCredentialProvider();
      credentialProvider.configure(config);
      restService.setBearerAuthCredentialProvider(credentialProvider);
    }
  }
}
