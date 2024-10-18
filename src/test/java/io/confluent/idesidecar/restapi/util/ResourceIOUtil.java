package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Objects;
import org.opentest4j.AssertionFailedError;
import wiremock.com.github.jknack.handlebars.internal.Files;

public class ResourceIOUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

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
   * the resource could not be found.
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
   * Serialize the supplied object to JSON.
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
   * @param query     the GraphQL query string
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
}
