package io.confluent.idesidecar.restapi.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface SchemaRegistryClient
    extends io.confluent.kafka.schemaregistry.client.SchemaRegistryClient {
  <T> T httpRequest(
      String path,
      String method,
      byte[] requestBodyData,
      Map<String, String> requestProperties,
      TypeReference<T> responseFormat
  ) throws IOException, RestClientException;

  default List<String> getSchemaTypes() throws RestClientException, IOException {
    return httpRequest(
        "/schemas/types",
        "GET",
        null,
        new HashMap<>(),
        new TypeReference<>() {
        }
    );
  }
}
