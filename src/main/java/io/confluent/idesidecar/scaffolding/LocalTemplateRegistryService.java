package io.confluent.idesidecar.scaffolding;

import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateNotFoundException;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryException;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryIOException;
import io.confluent.idesidecar.scaffolding.models.TemplateManifest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Template registry that manages templates on the local file system.
 */
public class LocalTemplateRegistryService implements TemplateRegistryService {

  public static final String TEMPLATES_DIR = "templates";

  private static final String MANIFEST_FILE_NAME = "manifest.yml";
  private static final String ALT_MANIFEST_FILE_NAME = "manifest.yaml";
  private static final String MANIFEST_READ_FAILED = "manifest_read_failed";
  private static final String REGISTRY_READ_FAILED = "registry_read_failed";
  private static final String TEMPLATE_READ_FAILED = "template_read_failed";

  protected final Path registryDir;

  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  // Store the template manifests in a list instead of a map.
  // We don't expect this list to get large enough to benefit from the O(1) lookup time
  // of a map.
  private List<TemplateManifest> templateManifests = new ArrayList<>();

  public LocalTemplateRegistryService(Path registryDir) {
    this.registryDir = registryDir;
  }

  @Override
  public void onStartup() throws TemplateRegistryException {
    if (registryDir == null) {
      throw new TemplateRegistryException(
          "Registry directory not set. "
              + "Please set the registry directory before starting the service",
          REGISTRY_READ_FAILED
      );
    }

    parseRegistryManifests();
  }

  @Override
  public List<TemplateManifest> listTemplates() {
    return List.copyOf(templateManifests);
  }

  @Override
  public TemplateManifest getTemplate(String templateName) throws TemplateNotFoundException {
    return templateManifests.stream()
        .filter(manifest -> manifest.name().equals(templateName))
        .findFirst()
        .orElseThrow(() -> new TemplateNotFoundException(
            "Template '%s' not found in registry".formatted(templateName)));
  }


  private void parseRegistryManifests() throws TemplateRegistryException {
    // Start with an empty list to avoid stale data
    List<TemplateManifest> newTemplateManifests = new ArrayList<>();
    var templatesDir = registryDir.resolve(TEMPLATES_DIR);
    if (!Files.exists(templatesDir)) {
      throw new TemplateRegistryException(
          "Required 'templates' directory not found in registry", REGISTRY_READ_FAILED);
    }

    // Look for manifests one level deep in the templates directory at the following path:
    // /templates/<template-name>/manifest.yml (or manifest.yaml)
    try (Stream<Path> templateFiles = Files.list(templatesDir)) {
      var templateDirs = templateFiles.filter(Files::isDirectory);
      for (Path templateDir : templateDirs.toList()) {
        newTemplateManifests.add(parseTemplateManifest(templateDir));
      }
    } catch (IOException e) {
      throw new TemplateRegistryException(
          "Failed to list directories", REGISTRY_READ_FAILED, e);
    }

    // Hot swap the template list
    templateManifests = newTemplateManifests;
  }

  private TemplateManifest parseTemplateManifest(Path directory) throws TemplateRegistryException {
    Path manifestFile = directory.resolve(MANIFEST_FILE_NAME);
    if (!Files.exists(manifestFile)) {
      // Maybe the manifest file is a `.yaml` file
      manifestFile = directory.resolve(ALT_MANIFEST_FILE_NAME);
    }

    if (Files.exists(manifestFile)) {
      try {
        return YAML_MAPPER.readValue(Files.readString(manifestFile), TemplateManifest.class);
      } catch (DatabindException e) {
        throw new TemplateRegistryException(
            "Failed to parse manifest file: %s".formatted(manifestFile),
            MANIFEST_READ_FAILED, e);
      } catch (IOException e) {
        throw new TemplateRegistryException(
            "Failed to read manifest file: %s".formatted(manifestFile),
            MANIFEST_READ_FAILED, e);
      }
    } else {
      throw new TemplateRegistryException(
          "Manifest file not found in template directory: %s".formatted(directory),
          // Worded "failed" instead of "manifest not found" to indicate
          // that this is an unexpected error condition and not a normal
          // resource not found condition.
          MANIFEST_READ_FAILED
      );
    }
  }

  /**
   * The template files are expected to be in a directory structure as follows:
   * <pre>
   *  templates/
   *    python-consumer/
   *      manifest.yml
   *      src/
   *        file1
   *        file2
   *      static/
   *        file3
   *        file4
   *  </pre>
   */
  @Override
  public Map<String, byte[]> getTemplateSrcContents(String templateName)
      throws TemplateRegistryIOException {
    return getSrcDirStream(templateName).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  @Override
  public Map<String, byte[]> getTemplateStaticContents(String templateName)
      throws TemplateRegistryIOException {
    return getStaticDirStream(templateName)
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  private Stream<Entry<String, byte[]>> getSrcDirStream(String templateName)
      throws TemplateRegistryIOException {
    return streamContentsAsBytes(getTemplateSrcDir(templateName));
  }

  private Stream<Entry<String, byte[]>> getStaticDirStream(String templateName)
      throws TemplateRegistryIOException {
    var staticDir = getTemplateStaticDir(templateName);
    // Handle the case where the static/ directory may not exist
    return Files.exists(staticDir) ? streamContentsAsBytes(staticDir) : Stream.empty();
  }

  private Path getTemplateSrcDir(String templateName) throws TemplateRegistryIOException {
    return registryDir.resolve(Path.of(TEMPLATES_DIR, templateName, "src"));
  }

  private Path getTemplateStaticDir(String templateName) throws TemplateRegistryIOException {
    return registryDir.resolve(Path.of(TEMPLATES_DIR, templateName, "static"));
  }

  /**
   * Stream the contents of a directory as byte arrays.
   */
  private static Stream<Entry<String, byte[]>> streamContentsAsBytes(Path directory)
      throws TemplateRegistryIOException {
    return streamContents(directory, path -> {
      try {
        return Files.readAllBytes(path);
      } catch (IOException e) {
        throw new TemplateRegistryIOException(
            "Failed to read template file: " + path, TEMPLATE_READ_FAILED, e);
      }
    });
  }

  /**
   * Stream the contents of a directory, applying a reader function to each file.
   *
   * @return stream of entries where the key is the relative path of the file and
   *         the value is the result of applying the reader function to the file.
   */
  private static <T> Stream<Entry<String, T>> streamContents(
      Path directory, Function<Path, T> reader
  ) throws TemplateRegistryIOException {
    try (Stream<Path> stream = Files.walk(directory)) {
      return stream
          .filter(Files::isRegularFile)
          .collect(Collectors.toMap(path -> directory.relativize(path).toString(), reader))
          .entrySet()
          .stream();
    } catch (IOException e) {
      throw new TemplateRegistryIOException(
          "Failed to access template contents: " + directory, TEMPLATE_READ_FAILED, e);
    }
  }
}
