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

    List<Error> validateTlsPemPaths() {
      return this.tlsPemPaths.stream()
          .filter(pemPath -> Files.notExists(Path.of(pemPath)))
          .map(pemPath ->
              new Error(
                  "cert_not_found",
                  "Cert file cannot be found",
                  "The cert file %s cannot be found.".formatted(pemPath),
                  "/spec/tls_pem_paths"

              )
          )
          .toList();
    }
  }

  public record PreferencesMetadata(
      @JsonProperty(required = true) String self
  ) {

  }
}