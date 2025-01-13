package io.confluent.idesidecar.restapi.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Ticker;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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
      Map<String, String> httpHeaders) {
    super(restService, cacheCapacity, providers, configs, httpHeaders, Ticker.systemTicker());
    this.restService = restService;
  }

  public SidecarSchemaRegistryClient(RestService restService, int cacheCapacity) {
    super(restService, cacheCapacity);
    this.restService = restService;
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
}
