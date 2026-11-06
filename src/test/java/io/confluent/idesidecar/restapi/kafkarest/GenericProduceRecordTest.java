package io.confluent.idesidecar.restapi.kafkarest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.idesidecar.restapi.clients.ClientConfigurator;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.exceptions.ExceptionMappers;
import io.confluent.idesidecar.restapi.util.ObjectMapperFactory;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.BadRequestException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GenericProduceRecordTest {

  private GenericProduceRecord produceRecord;

  @BeforeEach
  void setUp() {
    var clientConfigurator = mock(ClientConfigurator.class);
    when(clientConfigurator.getSerdeConfigs(any(), anyBoolean()))
        .thenThrow(new IllegalArgumentException("invalid record"));

    var recordSerializer = new RecordSerializer();
    recordSerializer.clientConfigurator = clientConfigurator;

    produceRecord = new TestProduceRecord();
    produceRecord.schemaRegistryClients = mock(SchemaRegistryClients.class);
    produceRecord.schemaManager = new SchemaManager();
    produceRecord.recordSerializer = recordSerializer;
  }

  @Test
  void shouldIdentifyKeySerializationFailures() {
    var error = produceAndMapError(data("invalid"), data(null));

    assertEquals("key", error.path("message_part").asText());
    assertEquals(400, error.path("error_code").asInt());
    assertEquals(
        "Failed to serialize key when producing message to topic topic: invalid record"
            + " caused by: invalid record",
        error.path("message").asText()
    );
  }

  @Test
  void shouldIdentifyValueSerializationFailures() {
    var error = produceAndMapError(data(null), data("invalid"));

    assertEquals("value", error.path("message_part").asText());
    assertEquals(400, error.path("error_code").asInt());
    assertEquals(
        "Failed to serialize value when producing message to topic topic: invalid record"
            + " caused by: invalid record",
        error.path("message").asText()
    );
  }

  @Test
  void shouldIdentifyKeyAndValueSerializationFailures() {
    var error = produceAndMapError(data("invalid-key"), data("invalid-value"));

    assertEquals("both", error.path("message_part").asText());
    assertEquals(400, error.path("error_code").asInt());
  }

  @Test
  void shouldNotIdentifyUnrelatedBadRequestsAsSerializationFailures() {
    var response = new ExceptionMappers().mapBadRequestException(
        new BadRequestException("unrelated")
    );
    var error = ObjectMapperFactory.getObjectMapper().valueToTree(response.getEntity());

    assertFalse(error.has("message_part"));
    assertEquals("unrelated", error.path("message").asText());
  }

  private JsonNode produceAndMapError(
      ProduceRequestData key,
      ProduceRequestData value
  ) {
    var request = new ProduceRequest(null, List.of(), key, value, null);
    var exception = assertThrows(
        BadRequestException.class,
        () -> produceRecord
            .produce("connection", "cluster", "topic", true, request)
            .await()
            .indefinitely()
    );
    var response = new ExceptionMappers().mapBadRequestException(exception);
    return ObjectMapperFactory.getObjectMapper().valueToTree(response.getEntity());
  }

  private static ProduceRequestData data(Object data) {
    return new ProduceRequestData(null, null, null, null, null, null, data);
  }

  private static final class TestProduceRecord extends GenericProduceRecord {

    @Override
    protected Uni<ProduceContext> sendSerializedRecord(ProduceContext context) {
      throw new AssertionError("dry-run must not send records");
    }
  }
}
