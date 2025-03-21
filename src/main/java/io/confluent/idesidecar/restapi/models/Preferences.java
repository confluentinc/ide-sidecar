package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.confluent.idesidecar.restapi.exceptions.InvalidPreferencesException;
import io.confluent.idesidecar.restapi.resources.PreferencesResource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.eclipse.microprofile.config.ConfigProvider;

@JsonPropertyOrder({
    "api_version",
    "kind",
    "metadata",
    "spec"
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Preferences(
    @JsonProperty(value = "api_version", required = true) String apiVersion,
    @JsonProperty(required = true) String kind,
    @JsonProperty PreferencesMetadata metadata,
    @JsonProperty(required = true) PreferencesSpec spec
) {

  public Preferences(PreferencesSpec spec) {
    this(
        ConfigProvider.getConfig().getValue("ide-sidecar.api.groupWithVersion", String.class),
        "Preferences",
        new PreferencesMetadata(
            ConfigProvider.getConfig().getValue("ide-sidecar.api.host", String.class)
                + PreferencesResource.API_RESOURCE_PATH
        ),
        spec
    );
  }

  @JsonPropertyOrder({
      "tls_pem_paths",
      "trust_all_certificates"
  })
  public record PreferencesSpec(
      @JsonProperty("tls_pem_paths") List<String> tlsPemPaths,
      @JsonProperty("trust_all_certificates") Boolean trustAllCertificates
  ) {

    public PreferencesSpec(
        List<String> tlsPemPaths,
        Boolean trustAllCertificates
    ) {
      this.tlsPemPaths = tlsPemPaths != null ? tlsPemPaths : List.of();
      this.trustAllCertificates = trustAllCertificates != null ? trustAllCertificates : false;
    }

    /**
     * Validates if all provided preferences are valid.
     *
     * @throws InvalidPreferencesException if any of the preferences are invalid
     */
    public void validate() throws InvalidPreferencesException {
      var errors = validateTlsPemPaths();

      if (!errors.isEmpty()) {
        throw new InvalidPreferencesException(errors);
      }
    }

    /**
     * Validates the TLS PEM paths provided in the preferences. Checks if the provided paths exist
     * in the file system and are not empty.
     *
     * @return a list of errors if any of the TLS PEM paths are invalid
     */
    List<Error> validateTlsPemPaths() {
      return this.tlsPemPaths.stream()
          .flatMap(pemPath -> {
            if (pemPath == null || pemPath.isBlank()) {
              return Stream.of(
                  new Error(
                      "cert_path_empty",
                      "Cert file path is null or empty",
                      "The cert file path cannot be null or empty.",
                      "/spec/tls_pem_paths"
                  )
              );
            } else if (Files.notExists(Path.of(pemPath))) {
              return Stream.of(
                  new Error(
                      "cert_not_found",
                      "Cert file cannot be found",
                      "The cert file '%s' cannot be found.".formatted(pemPath),
                      "/spec/tls_pem_paths"
                  )
              );
            } else {
              return Stream.empty();
            }
          })
          .toList();
    }
  }

  public record PreferencesMetadata(
      @JsonProperty(required = true) String self
  ) {

  }
}