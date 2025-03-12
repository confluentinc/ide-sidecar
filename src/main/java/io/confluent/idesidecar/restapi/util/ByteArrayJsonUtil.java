package io.confluent.idesidecar.restapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Utility class for encoding and decoding byte arrays as JSON objects. Modeled after how Confluent
 * Cloud Kafka REST API encodes byte arrays in JSON using a single field named "__raw__" with a
 * base64-encoded value.
 */
public final class ByteArrayJsonUtil {

  public static final String RAW_FIELD = "__raw__";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final CharsetDecoder UTF8_DECODER = StandardCharsets.UTF_8.newDecoder();

  public static byte[] asBytes(JsonNode node) {
    if (smellsLikeBytes(node)) {
      var rawValue = node.get(RAW_FIELD).asText();
      return Base64.getDecoder().decode(rawValue);
    } else {
      throw new IllegalArgumentException(
          "Expected a single field named " + RAW_FIELD + " with a base64-encoded value"
      );
    }
  }

  /**
   * Returns true if the given JSON node looks like a byte array encoded as a JSON object. Meaning
   * it is an object with a single field named {@code __raw__}.
   *
   * @param node The JSON node to check.
   * @return true if the node looks like a byte array.
   */
  public static boolean smellsLikeBytes(JsonNode node) {
    return node.isObject() && node.size() == 1 && node.has(RAW_FIELD);
  }

  public static JsonNode asJsonNode(byte[] bytes) {
    var node = OBJECT_MAPPER.createObjectNode();
    node.put(RAW_FIELD, Base64.getEncoder().encodeToString(bytes));
    return node;
  }

  /**
   * Use the UTF-8 decoder to convert the given bytes to a string.
   *
   * @param bytes The bytes to convert.
   * @return The string representation of the bytes.
   * @throws CharacterCodingException if the bytes are not valid UTF-8
   */
  public static String asUTF8String(byte[] bytes) throws CharacterCodingException {
    return UTF8_DECODER.decode(java.nio.ByteBuffer.wrap(bytes)).toString();
  }

  private ByteArrayJsonUtil() {
    // Utility class
  }
}
