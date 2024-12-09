package io.confluent.idesidecar.scaffolding;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.confluent.idesidecar.scaffolding.models.TemplateManifest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TemplateManifestTest {

  @BeforeAll
  static void setUp() {
    System.setProperty("ide-sidecar.template-registries.supported-languages",
        "java,python,go,node");
  }

  @Test
  void shouldParseValidManifest() {
    // When a valid manifest is loaded
    var validManifest = loadResource("static/valid-manifest.yml");

    // Then parsing it results in expected fields
    TemplateManifest manifest = parseManifest(validManifest);

    assertEquals("0.0.1", manifest.templateApiVersion());
    assertEquals("python-consumer", manifest.name());
    assertEquals("Python Consumer Application", manifest.displayName());
    assertEquals(
        "A simple Python consumer that reads messages from a topic in Confluent Cloud."
            + " Ideal for developers new to Kafka who want to learn about stream processing with Kafka."
            + "\n",
        manifest.description());
    assertEquals("Python", manifest.language());
    assertEquals(List.of("consumer", "getting started", "python"), manifest.tags());
    assertEquals("0.0.1", manifest.version());

    var manifestOptions = manifest.options();
    assertNull(manifestOptions.get("api_key").initialValue());
    assertNull(manifestOptions.get("api_secret").initialValue());
    assertNull(manifestOptions.get("topic").initialValue());
    assertNull(manifestOptions.get("group_id").initialValue());
    assertEquals(
        "earliest",
        manifestOptions.get("auto_offset_reset").initialValue()
    );
    assertEquals(
        "localhost:9092,localhost:9093,localhost:9094",
        manifestOptions.get("bootstrap_server").initialValue()
    );
  }

  protected

  @Test
  void shouldFailToParseManifestWithInvalidVersion() {
    String manifestFileContents = """
        template_api_version: 0.0.1
        name: python-consumer
        display_name: Python Consumer
        description: Awesome template for a simple Python consumer application.
        language: python
        tags:
          - consumer
          - getting started
          - python
        version: im-a-little-teapot
        """;

    assertFailToLoadInvalidManifest(
        manifestFileContents,
        ValueInstantiationException.class,
        "Version must be a valid SemVer string."
    );
  }

  @Test
  void shouldFailToParseManifestWithMissingMandatoryFields() {
    String manifestFileContents = """
        template_api_version: 0.0.1
        name: python-consumer
        display_name: Python Consumer
        description: Awesome template for a simple Python consumer application.
        language: Python
        version: 0.0.1
        """;

    assertFailToLoadInvalidManifest(
        manifestFileContents,
        MismatchedInputException.class,
        "Missing required creator property 'tags'"
    );
  }

  @Test
  void shouldParseManifestWithOnlyRequiredFields() {
    // When the manifest has only required fields
    String manifestFileContents = """
        template_api_version: 0.0.1
        name: python-consumer
        display_name: Python Consumer
        description: Awesome template for a simple Python consumer application.
        language: python
        tags:
          - consumer
          - getting started
          - python
        version: 0.0.1
        """;

    // Then the template can be loaded
    TemplateManifest manifest = parseManifest(manifestFileContents);

    // And the options are non-null and empty
    assertEquals(Map.of(), manifest.options());
  }

  @Test
  void shouldParseManifestWithEmptyTagsAndOptions() {
    String manifestFileContents = """
        template_api_version: 0.0.1
        name: python-consumer
        display_name: Python Consumer
        description: Awesome template for a simple Python consumer application.
        language: python
        version: 0.0.1
        tags: []
        options: {}
        """;

    TemplateManifest manifest = parseManifest(manifestFileContents);

    assertEquals(List.of(), manifest.tags());
    assertEquals(Map.of(), manifest.options());
  }

  @Test
  void shouldFailToLoadInvalidYaml() {
    String manifestFileContents = """
        template_api_version: 0.0.1
        name: python-consumer
        display_name: Python Consumer
        description: Awesome template for a simple Python consumer application.
        language: python
        version: 0.0.1
        tags: []
        options: {}
        !!! wowowow such invalid yaml
        """;

    assertFailToLoadInvalidYaml(manifestFileContents);
  }

  @Test
  void shouldParseManifestWithoutOptions() {

    // When a manifest has no options
    String manifestFileContents = """
        template_api_version: 0.0.1
        name: python-consumer
        display_name: Python Consumer
        description: Awesome template for a simple Python consumer application.
        language: python
        tags:
          - consumer
          - getting started
          - python
        version: 0.0.1
        """;

    // Then the template can be loaded
    TemplateManifest manifest = parseManifest(manifestFileContents);

    assertEquals(
        manifest.options(),
        Map.of()
    );
  }

  @Test
  void validateValuesShouldReturnNoErrorIfAllValuesAreValid() {
    // When a valid manifest is loaded
    var validManifest = loadResource("static/valid-manifest.yml");

    // Then parsing it results in expected fields
    var manifest = parseManifest(validManifest);
    Map<String, Object> values = Map.of(
        "bootstrap_server", "localhost:9092",
        "api_key", "key",
        "api_secret", "secret",
        "topic", "dtx",
        "group_id", "",
        "auto_offset_reset", "earliest"
    );

    assertTrue(manifest.validateValues(values).isEmpty());
  }

  @Test
  void validateValuesShouldReturnErrorIfValueIsMissing() {
    // When a valid manifest is loaded
    var validManifest = loadResource("static/valid-manifest.yml");

    // Then parsing it results in expected fields
    var manifest = parseManifest(validManifest);
    Map<String, Object> values = Map.of(
        "bootstrap_server", "localhost:9092",
        "api_key", "key",
        "api_secret", "secret",
        "topic", "dtx",
        "group_id", ""
    );
    var errors = manifest.validateValues(values);

    assertFalse(errors.isEmpty());
    assertEquals(
        "Required option auto_offset_reset is not provided.",
        errors.getFirst().detail()
    );
  }

  @Test
  void validateValuesShouldReturnErrorIfOptionIsUnsupported() {
    // When a valid manifest is loaded
    var validManifest = loadResource("static/valid-manifest.yml");

    // Then parsing it results in expected fields
    var manifest = parseManifest(validManifest);
    Map<String, Object> values = Map.of(
        "bootstrap_server", "localhost:9092",
        "api_key", "key",
        "api_secret", "secret",
        "topic", "dtx",
        "group_id", "",
        "auto_offset_reset", "earliest",
        "unsupported_option", ""
    );
    var errors = manifest.validateValues(values);

    assertFalse(errors.isEmpty());
    assertEquals(
        "Provided option unsupported_option is not supported by the template.",
        errors.getFirst().detail()
    );
  }

  @Test
  void validateValuesShouldReturnErrorIfValueViolatesMinLengthConstraint() {
    // When a valid manifest is loaded
    var validManifest = loadResource("static/valid-manifest.yml");

    // Then parsing it results in expected fields
    var manifest = parseManifest(validManifest);
    Map<String, Object> values = Map.of(
        "bootstrap_server", "",
        "api_key", "key",
        "api_secret", "secret",
        "topic", "dtx",
        "group_id", "",
        "auto_offset_reset", "earliest"
    );
    var errors = manifest.validateValues(values);

    assertFalse(errors.isEmpty());
    assertEquals(
        "The provided value has 0 characters but the option bootstrap_server requires at "
            + "least 1 character(s).",
        errors.getFirst().detail()
    );
  }

  String loadResource(String path) {
    try {
      var validManifestContent = Thread
          .currentThread()
          .getContextClassLoader()
          .getResourceAsStream(path);
      assertNotNull(validManifestContent);
      return new String(validManifestContent.readAllBytes());
    } catch (IOException e) {
      fail("Unable to read resource file at path: " + path, e);
      return null;
    }
  }

  TemplateManifest parseManifest(String manifestContents) {
    try {
      var mapper = new ObjectMapper(new YAMLFactory());
      TemplateManifest manifest = mapper.readValue(manifestContents, TemplateManifest.class);
      assertNotNull(manifest);
      return manifest;
    } catch (JsonProcessingException e) {
      fail("Expected valid manifest", e);
      return null;
    }
  }

  void assertFailToLoadInvalidYaml(String invalidManifestContents) {
    var mapper = new ObjectMapper(new YAMLFactory());
    assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(invalidManifestContents, TemplateManifest.class)
    );
  }

  <ErrorT extends Throwable> void assertFailToLoadInvalidManifest(
      String invalidManifest,
      Class<ErrorT> expectedException,
      String errorMessageSubstring
  ) {
    var mapper = new ObjectMapper(new YAMLFactory());
    var exception = assertThrows(expectedException,
        () -> mapper.readValue(invalidManifest, TemplateManifest.class));
    assertTrue(exception.getMessage().contains(errorMessageSubstring));
  }
}
