package io.confluent.idesidecar.scaffolding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.mustachejava.MustacheException;
import io.confluent.idesidecar.scaffolding.MustacheTemplateEngine;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MustacheTemplateEngineTest {

  @Test
  void testBasicRendering() {
    var template = Map.of("foo.txt", "{{key1}} {{key2}}".getBytes());
    Map<String, Object> replacements = Map.of("key1", "value1", "key2", "value2");

    var engine = new MustacheTemplateEngine();
    var rendered = engine.renderTemplate(template, replacements);

    assertEquals("value1 value2", new String(rendered.get("foo.txt")));
  }

  @Test
  void testRenderingEmptyMustacheThrowsException() {
    var template = Map.of("key1", "{{}}".getBytes());
    Map<String, Object> replacements = Map.of("key1", "value1");

    var engine = new MustacheTemplateEngine();
    var exception = assertThrows(MustacheException.class,
        () -> engine.renderTemplate(template, replacements));
    assertTrue(exception.getMessage().contains("Empty mustache"));
  }

  @Test
  void testFilenamesAreRendered() {
    var template = Map.of("{{key1}}.txt", "{{key1}} {{key2}}".getBytes());
    Map<String, Object> replacements = Map.of("key1", "value1", "key2", "value2");

    var engine = new MustacheTemplateEngine();
    var rendered = engine.renderTemplate(template, replacements);

    assertEquals("value1.txt", rendered.keySet().iterator().next());
  }
}
