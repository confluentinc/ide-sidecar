package io.confluent.idesidecar.restapi.clients;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchemaRegistryClientsCacheTest {

  // more distinct schema IDs than the old capacity of 10, well under the new capacity
  private static final int SCHEMA_ID_COUNT = 50;

  @Test
  void shouldServeRepeatedSchemaLookupsFromCache() throws Exception {
    var restService = mock(RestService.class);
    when(restService.getId(anyInt(), anyString())).thenReturn(new SchemaString("\"string\""));
    var client = SchemaRegistryClients.newCachingClient(restService, Map.of());

    for (var pass = 0; pass < 2; pass++) {
      for (var id = 1; id <= SCHEMA_ID_COUNT; id++) {
        client.getSchemaById(id);
      }
    }

    for (var id = 1; id <= SCHEMA_ID_COUNT; id++) {
      verify(restService, times(1)).getId(id, "");
    }
  }
}
