package io.confluent.idesidecar.restapi.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class FailureTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void shouldParseCCloudCMKFailureMessage() throws IOException {
    var json = """
        {
          "errors": [
            {
              "id": "a6f92dfec830d00ae4a5c2d7a65ba4ed",
              "status": "404",
              "detail": "Not found",
              "source": {}
            }
          ]
        }
        """;
    var failure = OBJECT_MAPPER.readValue(json, Failure.class);
    assertEquals(1, failure.errors().size());
    assertNull(failure.title());
    assertNull(failure.code());
    assertNull(failure.status());
    var error = failure.errors().getFirst();
    assertEquals("Not found", error.detail());
    assertEquals("404", error.status());
    assertEquals("a6f92dfec830d00ae4a5c2d7a65ba4ed", error.id());
    assertNull(error.title());
    assertNull(error.code());
    var source = error.source();
    assertInstanceOf(ObjectNode.class, source);
    assertTrue(source.isEmpty());
  }

}