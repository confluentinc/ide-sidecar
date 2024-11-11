package io.confluent.idesidecar.restapi.credentials;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

class RedactedTestBase<T> {

  static final ObjectMapper MAPPER = new ObjectMapper();

  void assertSerializeAndDeserialize(String path, String pathToRedacted, Class<T> valueType) {
    T parsed = readValueFromResource(path, valueType);
    T expected = readValueFromResource(pathToRedacted, valueType);

    // The serialized form should be redacted
    String serialized = serialize(parsed);
    T deserialized = deserialize(serialized, valueType);
    assertEquals(expected, deserialized);

    // If we serialize the (redacted) deserialize value and deserialize it again, it will be reacted
    String reserialized = serialize(deserialized);
    assertEquals(reserialized, serialized);
  }

  T readValueFromResource(String path, Class<T> valueType) {
    var inputJson = asJson(
        loadResource(path)
    );
    try {
      return MAPPER.treeToValue(inputJson, valueType);
    } catch (IOException e) {
      fail("unable to read value from resource file " + path, e);
      return null;
    }
  }

  String serialize(T value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      fail("unable to serialize value " + value, e);
      return null;
    }
  }

  T deserialize(String jsonString, Class<T> valueType) {
    try {
      return MAPPER.readValue(jsonString, valueType);
    } catch (JsonProcessingException e) {
      fail("unable to serialize value " + jsonString, e);
      return null;
    }
  }
}