package io.confluent.idesidecar.scaffolding;

import static io.confluent.idesidecar.scaffolding.util.PortablePathUtil.portablePath;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Template engine that uses <a href="https://mustache.github.io/">Mustache</a> to expand tags and
 * replace placeholders.
 */
public class MustacheTemplateEngine implements TemplateEngine {

  private static final MustacheFactory MUSTACHE_FACTORY = new DefaultMustacheFactory();
  private static final Charset CHARSET = StandardCharsets.UTF_8;

  @Override
  public Map<String, byte[]> renderTemplateFromRegistry(
      TemplateRegistryService templateRegistryService,
      String templateName,
      Map<String, Object> options
  ) throws TemplateRegistryException {

    var templateManifest = templateRegistryService.getTemplate(templateName);
    var templateOptions = templateManifest.populateOptionsWithDefaults(options);

    var templateSrcContents = templateRegistryService.getTemplateSrcContents(templateName);
    var renderedSrcContents = renderTemplate(templateSrcContents, templateOptions);
    var templateStaticContents = templateRegistryService.getTemplateStaticContents(templateName);

    return Stream
        .concat(
            renderedSrcContents.entrySet().stream(),
            templateStaticContents.entrySet().stream()
        )
        .collect(
            Collectors.toMap(
                Entry::getKey,
                Entry::getValue,
                // Overwrite the static content with the rendered content
                // in case of a collision of file paths
                (srcContent, staticContent) -> staticContent
            )
        );
  }

  /**
   * Renders the template contents using the provided options. Made public for testing purposes.
   */
  public Map<String, byte[]> renderTemplate(Map<String, byte[]> templateContents,
      Map<String, Object> templateOptions) {
    Map<String, byte[]> renderedTemplates = new HashMap<>();
    for (var entry : templateContents.entrySet()) {
      String filePath = entry.getKey();

      // Use UTF-8 encoding to read the template content bytes
      String templateContent = new String(entry.getValue(), CHARSET);

      // We pass `null` here for the file name -- studying the source code of
      // DefaultMustacheFactory, it seems the name has no functional effect on the rendering,
      // the author of the module seems to have added it for debugging purposes.
      // https://groups.google.com/g/mustachejava/c/lkqv0QYnUxo/m/Lq_ohPc7yfwJ
      var mustacheFilename = MUSTACHE_FACTORY.compile(new StringReader(filePath), null);
      var writerFilename = new StringWriter();
      // Render filename
      mustacheFilename.execute(writerFilename, templateOptions);

      var mustacheContent = MUSTACHE_FACTORY.compile(new StringReader(templateContent), null);
      var writerContent = new StringWriter();
      // Render content
      mustacheContent.execute(writerContent, templateOptions);

      renderedTemplates.put(
          portablePath(writerFilename.toString()),
          writerContent.toString().getBytes(CHARSET));
    }
    return renderedTemplates;
  }
}
