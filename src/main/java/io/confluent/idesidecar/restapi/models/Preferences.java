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
import java.util.Map;
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
      "kerberos_config_file_path",
      "tls_pem_paths",
      "trust_all_certificates",
      "flink_private_endpoints"
  })
  public record PreferencesSpec(
      @JsonProperty("kerberos_config_file_path") String kerberosConfigFilePath,
      @JsonProperty("tls_pem_paths") List<String> tlsPemPaths,
      @JsonProperty("trust_all_certificates") Boolean trustAllCertificates,
      @JsonProperty("flink_private_endpoints") Map<String, List<String>> flinkPrivateEndpoints
  ) {

    private static final String FLINK_PRIVATE_ENDPOINTS_PATH = "/spec/flink_private_endpoints";

    public PreferencesSpec(
        String kerberosConfigFilePath,
        List<String> tlsPemPaths,
        Boolean trustAllCertificates,
        Map<String, List<String>> flinkPrivateEndpoints
    ) {
      this.kerberosConfigFilePath = kerberosConfigFilePath != null ? kerberosConfigFilePath : "";
      this.tlsPemPaths = tlsPemPaths != null ? tlsPemPaths : List.of();
      this.trustAllCertificates = trustAllCertificates != null ? trustAllCertificates : false;
      this.flinkPrivateEndpoints = flinkPrivateEndpoints != null ? flinkPrivateEndpoints : Map.of();
    }

    /**
     * Validates if all provided preferences are valid.
     *
     * @throws InvalidPreferencesException if any of the preferences are invalid
     */
    public void validate() throws InvalidPreferencesException {
      var errors = Stream.of(
          validateTlsPemPaths(),
          validateKerberosConfigFilePath(),
          validateFlinkPrivateEndpoints()
      ).flatMap(stream -> stream).toList();

      if (!errors.isEmpty()) {
        throw new InvalidPreferencesException(errors);
      }
    }

    /**
     * Validates the TLS PEM paths provided in the preferences. Checks if the provided paths exist
     * in the file system and are not empty.
     *
     * @return errors if any of the TLS PEM paths are invalid
     */
    Stream<Error> validateTlsPemPaths() {
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
          });
    }

    /**
     * Validates the Kerberos config file path provided in the preferences. Checks if the provided
     * path exists in the file system.
     *
     * @return the error if the Kerberos config file path is invalid
     */
    Stream<Error> validateKerberosConfigFilePath() {
      if (kerberosConfigFilePath != null
          && !kerberosConfigFilePath.isBlank()
          && Files.notExists(Path.of(kerberosConfigFilePath))
      ) {
        return Stream.of(
            new Error(
                "krb5_config_file_not_found",
                "Kerberos config file cannot be found",
                "The Kerberos config file '%s' cannot be found.".formatted(kerberosConfigFilePath),
                "/spec/kerberos_config_file_path"
            )
        );
      } else {
        return Stream.empty();
      }
    }

    /**
     * Validates the private endpoints map. Checks if the provided
     * endpoints are not null or empty.
     *
     * @return errors if any private endpoints are invalid
     */
    Stream<Error> validateFlinkPrivateEndpoints() {
      if (flinkPrivateEndpoints == null || flinkPrivateEndpoints.isEmpty()) {
        return Stream.empty();
      }

      for (var entry : flinkPrivateEndpoints.entrySet()) {
        String envId = entry.getKey();
        List<String> endpoints = entry.getValue();

        if (envId == null || envId.isBlank()) {
          return Stream.of(
              new Error(
                  "private_endpoint_empty_key",
                  "Environment ID cannot be empty",
                  "Environment ID key cannot be null or empty.",
                  FLINK_PRIVATE_ENDPOINTS_PATH
              )
          );
        }

        for (String endpoint : endpoints) {
          if (endpoint == null || endpoint.isBlank()) {
            return Stream.of(
                new Error(
                    "private_endpoint_empty_value",
                    "Private endpoint cannot be empty",
                    "Private endpoint in environment '%s' cannot be null or empty.".formatted(envId),
                    FLINK_PRIVATE_ENDPOINTS_PATH
                )
            );
          }
        }
      }

      return Stream.empty();
    }
  }

  public record PreferencesMetadata(
      @JsonProperty(required = true) String self
  ) {

  }
}
