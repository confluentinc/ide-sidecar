package io.confluent.idesidecar.restapi.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MockSchemaRegistryClient
    extends io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
    implements SchemaRegistryClient {

  @Override
  public <T> T httpRequest(String path, String method, byte[] requestBodyData, Map<String, String> requestProperties, TypeReference<T> responseFormat) throws IOException, RestClientException {
    return null;
  }

  @Override
  public List<String> getSchemaTypes() throws RestClientException, IOException {
    return SchemaRegistryClient.super.getSchemaTypes();
  }
}
