package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import org.opentest4j.AssertionFailedError;
import wiremock.com.github.jknack.handlebars.internal.Files;

public class ResourceIOUtil {

  private static final ObjectMapper MAPPER = createObjectMapper();

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    // Automatically registers JavaTimeModule and others as would be done for the ObjectMapper
    // used within Quarkus will be. Alas that ResourceIOUtil is used as collection
    // of static methods, @Inject is not possible here.
    mapper.findAndRegisterModules();
    return mapper;
  }

  /**
   * Load a resource file at the given path relative to the current thread's context class loader.
   * Typically, this is a path relative to the `src/test/resources` folder in the source code.
   *
   * <p>This method is only to be used within tests, since it fails the test if
   * the resource could not be found.
   *
   * @param resourcePath the relative path to the resource file
   * @return the contents of the resource file
   * @throws AssertionFailedError if the resource file could not be found
   */
  public static byte[] loadResourceAsBytes(String resourcePath) {
    try {
      return Objects.requireNonNull(
          Thread.currentThread()
              .getContextClassLoader()
              .getResourceAsStream(resourcePath)
      ).readAllBytes();
    } catch (IOException e) {
      fail("Error loading resource file " + resourcePath, e);
      return null;
    }
  }

  public static String loadResource(String resourcePath) {
    return new String(
        Objects.requireNonNull(
            loadResourceAsBytes(resourcePath)
        )
    );
  }

  public static <T> T loadResourceAsObject(String resourcePath, Class<T> type) {
    try {
      return MAPPER.readValue(loadResource(resourcePath), type);
    } catch (IOException e) {
      fail("Error loading resource file " + resourcePath, e);
      return null;
    }
  }

  public static String loadFile(String relativePath) {
    var path = Path.of(relativePath).toAbsolutePath();
    try {
      return Objects.requireNonNull(
          Files.read(path.toFile(), StandardCharsets.UTF_8)
      );
    } catch (IOException e) {
      fail("Error loading file " + path, e);
      return null;
    }
  }

  /**
   * Parse the supplied string content as JSON.
   *
   * <p>This method is only to be used within tests, since it fails the test if
   * the JSON string content could not be parsed.
   *
   * @param content the JSON string content
   * @return the JSON representation; never null
   * @throws AssertionFailedError if the content could not be parsed as JSON
   */
  public static JsonNode asJson(String content) {
    try {
      return MAPPER.readTree(content);
    } catch (IOException e) {
      fail("Error parsing JSON", e);
      return null;
    }
  }

  /**
   * Parse the supplied string content as the supplied type.
   *
   * <p>This method is only to be used within tests, since it fails the test if
   * the JSON string content could not be parsed.
   *
   * @param content the JSON string content
   * @param type    the type of the object to instantiate
   * @return the object representation of the JSON; never null
   * @throws AssertionFailedError if the content could not be parsed as JSON
   */
  public static <T> T asObject(String content, Class<T> type) {
    try {
      return MAPPER.readValue(content, type);
    } catch (IOException e) {
      fail("Error parsing JSON", e);
      return null;
    }
  }

  /**
   * Serialize the supplied object to JSON.
   *
   * <p>This method is only to be used within tests, since it fails the test if
   * the object could not be written as JSON.
   *
   * @param object the object to be serialized
   * @return the JSON representation; never null
   * @throws AssertionFailedError if the content could not be parsed as JSON
   */
  public static JsonNode asJson(Object object) {
    try {
      String jsonString = MAPPER.writeValueAsString(object);
      return MAPPER.readTree(jsonString);
    } catch (JsonProcessingException e) {
      fail("Error converting to JSON: " + object, e);
      return null;
    }
  }

  /**
   * Construct a GraphQL request payload for a {@code POST} query request.
   *
   * @param query the GraphQL query string
   * @return the request payload; never null
   */
  public static String asGraphQLRequest(String query) {
    return asGraphQLRequest(query, null);
  }

  /**
   * Construct a GraphQL request payload for a {@code POST} query request.
   *
   * @param query     the GraphQL query string
   * @param variables the variables as a JSON object; may be null
   * @return the request payload; never null
   */
  public static String asGraphQLRequest(String query, JsonNode variables) {
    // Create the request
    if (variables == null || variables.isEmpty()) {
      variables = MAPPER.createObjectNode();
    }
    var doc = MAPPER.createObjectNode();
    doc.put("query", query);
    doc.put("variables", variables);
    return doc.toPrettyString();
  }

  /**
   * Serializes and deserializes the POJO of the given type. This also checks the equals, hashCode,
   * and toString methods, and if the object is comparable it checks the compareTo method.
   *
   * @param type the JSON POJO class to test
   * @param <T>  the type of JSON POJO class
   * @return the test function; never null
   */
  @SuppressWarnings("unchecked")
  public static <T> T deserializeAndSerialize(String resourcePath, Class<T> type) {
    try {
      var content = loadResource(resourcePath);
      var expected = MAPPER.readValue(content, type);
      serializeAndDeserialize(expected);
      return expected;
    } catch (IOException e) {
      fail("Error loading and deserializing " + type, e);
      return null;
    }
  }

  /**
   * Serializes and deserializes the POJO of the given type. This also checks the equals, hashCode,
   * and toString methods, and if the object is comparable it checks the compareTo method.
   *
   * @param expected the object to serialize and deserialize
   * @param <T>      the type of JSON POJO class
   */
  @SuppressWarnings("unchecked")
  public static <T> void serializeAndDeserialize(T expected) {
    try {
      // Serialize
      var serialized = MAPPER.writeValueAsString(expected);
      var deserialized = MAPPER.readValue(serialized, expected.getClass());

      boolean equals = expected.equals(deserialized);

      assertEquals(expected, deserialized);

      // Check equals, hashCode, toString
      assertSame(deserialized, deserialized);
      assertEquals(deserialized, deserialized);
      assertEquals(expected, deserialized);
      assertEquals(expected.hashCode(), deserialized.hashCode());
      assertEquals(expected.toString(), deserialized.toString());
      assertNotEquals("not the same", deserialized);
      assertNotEquals(deserialized, null);
      assertEquals(expected, expected); // always use 'equals' method
      if (expected instanceof Comparable<?> comparableExpected) {
        assertEquals(0, ((Comparable<T>) expected).compareTo(expected));
        assertEquals(0, ((Comparable<T>) expected).compareTo(expected));
      }
    } catch (IOException e) {
      fail("Error serializing and deserializing " + expected, e);
    }
  }
}
