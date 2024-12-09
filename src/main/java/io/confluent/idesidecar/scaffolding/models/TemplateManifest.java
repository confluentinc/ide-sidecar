package io.confluent.idesidecar.scaffolding.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.vdurmont.semver4j.Semver;
import com.vdurmont.semver4j.SemverException;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
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

      @JsonProperty(value = "initial_value")
      String initialValue,

      @JsonProperty(value = "min_length")
      Integer minLength
  ) {

  }

  /**
   * Validates if a set of values can be used to apply this template.
   *
   * @param values The provided values
   * @return the list of errors that occurred when validating the values; empty list if all values
   * are valid
   */
  public List<Error> validateValues(Map<String, Object> values) {
    var errors = valuesReferenceAllOptions(values);
    errors = Stream.concat(errors, valuesReferenceOnlySupportedOptions(values));
    errors = Stream.concat(errors, valuesComplyWithMinLengthConstraint(values));

    return errors.collect(Collectors.toList());
  }

  /**
   * Validates if a set of values references all options of this template.
   *
   * @param values The provided values
   * @return the list of errors containing one error for each option that is not referenced by a
   * value; empty list if all options are referenced
   */
  private Stream<Error> valuesReferenceAllOptions(Map<String, Object> values) {
    return options
        .keySet().stream()
        .flatMap(option -> {
          if (values.containsKey(option)) {
            return Stream.empty();
          } else {
            return Stream.of(
                new Error(
                    "missing_template_option",
                    "Missing template option",
                    "Required option %s is not provided.".formatted(option),
                    option
                )
            );
          }
        });
  }

  /**
   * Validates if a set of values references only supported options of this template.
   *
   * @param values The provided values
   * @return the list of errors containing one error for each value that references an option not
   * supported by this template; empty list if values reference only known options
   */
  private Stream<Error> valuesReferenceOnlySupportedOptions(Map<String, Object> values) {
    return values
        .keySet().stream()
        .flatMap(option -> {
          if (options.containsKey(option)) {
            return Stream.empty();
          } else {
            return Stream.of(
                new Error(
                    "unsupported_template_option",
                    "Unsupported template option",
                    "Provided option %s is not supported by the template.".formatted(option),
                    option
                )
            );
          }
        });
  }

  /**
   * Validates if a set of values complies with the <code>min_length</code> constraints of the
   * options of this template.
   *
   * @param values The provided values
   * @return the list of errors containing one error for each value that violates the
   * <code>min_length</code> constraint of the referenced option
   */
  private Stream<Error> valuesComplyWithMinLengthConstraint(Map<String, Object> values) {
    return values
        .entrySet().stream()
        .flatMap(entry -> {
          var optionName = entry.getKey();
          var option = options.get(optionName);
          var valueLength = entry.getValue().toString().length();
          if (
              option != null
                  && option.minLength != null
                  && valueLength < option.minLength
          ) {
            return Stream.of(
                new Error(
                    "template_option_violates_min_length",
                    "Template option violates min_length constraint",
                    String.format(
                        "The provided value has %d characters but the option %s requires at least"
                            + " %d character(s).",
                        valueLength,
                        optionName,
                        option.minLength
                    ),
                    optionName
                )
            );
          } else {
            return Stream.empty();
          }
        });
  }

  public record Error(String code, String title, String detail, String source) implements
      Serializable {

  }
}
