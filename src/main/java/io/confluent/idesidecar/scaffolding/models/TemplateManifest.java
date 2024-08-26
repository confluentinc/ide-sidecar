package io.confluent.idesidecar.scaffolding.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.SemverException;
import io.confluent.idesidecar.scaffolding.exceptions.InvalidTemplateOptionsProvided;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@RegisterForReflection
public record TemplateManifest(
    /*
    template_api_version, name, display_name, description, language, and version
    are mandatory fields. All other fields are optional.
     */
    @JsonProperty(value = "template_api_version", required = true)
    String templateApiVersion,
    @JsonProperty(required = true)
    String name,
    @JsonProperty(value = "display_name", required = true)
    String displayName,
    @JsonProperty(required = true)
    String description,
    @JsonProperty(required = true)
    String language,
    @JsonProperty(required = true)
    String version,
    @JsonProperty(required = true)
    List<String> tags,
    Map<String, OptionProperties> options
) {

  public TemplateManifest {
    validateVersion(version);
    if (tags == null) {
      tags = List.of();
    }
    if (options == null) {
      options = Map.of();
    }
  }

  private static void validateVersion(String version) {
    try {
      new Semver(version);
    } catch (SemverException e) {
      throw new IllegalArgumentException("Version must be a valid SemVer string.");
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record OptionProperties(
      @JsonProperty(value = "display_name", required = true)
      String displayName,

      @JsonProperty(value = "description", required = true)
      String description,

      @JsonProperty(value = "hint")
      String hint,

      @JsonProperty(value = "format")
      String format,

      @JsonProperty(value = "pattern")
      String pattern,

      @JsonProperty(value = "enum")
      List<String> enums,

      @JsonProperty(value = "default_value")
      Object defaultValue
  ) {
  }

  /**
   * Merge the provided options with the unprovided options that have default values in the template
   * manifest.
   *
   * <p>
   * For example, if the template manifest defines the following options:
   * <pre>
   * # file: manifest.yml
   * options:
   *   foo:
   *     default_value: foo-default
   *   bar:
   *     default_value: bar-default
   * </pre>
   * and the user provides only "bar", the resulting map will be:
   * <pre>
   * {
   *   "foo": "foo-default",
   *   "bar": "eggs"
   * }
   * </pre>
   * </p>
   *
   * @param providedOptions the options provided by the user
   * @return the populated options
   */
  public Map<String, Object> populateOptionsWithDefaults(Map<String, Object> providedOptions)
      throws InvalidTemplateOptionsProvided {
    var errors = validateProvidedOptions(providedOptions);
    if (!errors.isEmpty()) {
      throw new InvalidTemplateOptionsProvided(errors);
    }

    return options.entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        entry -> providedOptions.getOrDefault(entry.getKey(), entry.getValue().defaultValue())
    ));
  }

  private List<Error> validateProvidedOptions(Map<String, Object> providedOptions) {
    return Stream.concat(
            validateProvidedOptionsAreSupported(providedOptions).stream(),
            validateProvidedOptionsHaveDefaultValues(providedOptions).stream())
        .collect(Collectors.toList());
  }

  /**
   * Validate that all provided options are supported by the template.
   *
   * @param providedOptions the options provided by the user
   * @return a list of error messages
   */
  private List<Error> validateProvidedOptionsAreSupported(Map<String, Object> providedOptions) {
    return providedOptions.keySet().stream()
        .filter(key -> !options.containsKey(key))
        .map(key -> new Error(
            "unsupported_template_option",
            "Unsupported template option",
            "Provided option is not supported by the template manifest.",
            key
        )).collect(Collectors.toList());
  }

  /**
   * Validate that all options are provided or have default values configured in the template
   * manifest.
   *
   * @param providedOptions the options provided by the user
   * @return a list of error messages
   */
  private List<Error> validateProvidedOptionsHaveDefaultValues(
      Map<String, Object> providedOptions) {
    return options.entrySet().stream()
        .filter(entry ->
            !providedOptions.containsKey(entry.getKey())
                && Objects.isNull(entry.getValue().defaultValue()))
        .map(entry -> new Error(
            "missing_template_option",
            "Missing template option",
            "Template option not provided and has no default value in the manifest.",
            entry.getKey()))
        .collect(Collectors.toList());
  }

  public record Error(String code, String title, String detail, String source) implements
      Serializable {

  }
}
