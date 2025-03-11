package io.confluent.idesidecar.restapi.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A custom interface for implementing methods that are missing from the underlying
 * {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient}.
 * <li>
 *   <ul>{@link #httpRequest(String, String, byte[], Map, TypeReference)}: A method for making
 *   generic HTTP requests to the Schema Registry server.</ul>
 *   <ul>{@link #getSchemaTypes()}: A method for getting the list of schema types supported by the
 *   Schema Registry server.</ul>
 * </li>
 */
public interface SchemaRegistryClient extends
    io.confluent.kafka.schemaregistry.client.SchemaRegistryClient {

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
