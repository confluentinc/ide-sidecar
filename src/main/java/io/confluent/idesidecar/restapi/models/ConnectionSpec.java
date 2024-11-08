package io.confluent.idesidecar.restapi.models;

import static io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType.CCLOUD;
import static io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType.DIRECT;
import static io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType.LOCAL;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.idesidecar.restapi.credentials.ApiKeyAndSecret;
import io.confluent.idesidecar.restapi.credentials.BasicCredentials;
import io.confluent.idesidecar.restapi.credentials.Credentials;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.confluent.idesidecar.restapi.util.CCloud.KafkaEndpoint;
import io.confluent.idesidecar.restapi.util.CCloud.SchemaRegistryEndpoint;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Null;
import jakarta.validation.constraints.Size;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "The connection details that can be set or changed.")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectionSpec(
    @Schema(description = "The unique identifier of the connection resource.")
    @Size(min = 1, max = 64)
    String id,
    @Schema(description = "The user-supplied name of the connection resource.")
    @Size(max = 64)
    String name,
    @Schema(description = "The type of connection resource.")
    ConnectionType type,
    @Schema(description = "The details for connecting to CCloud.")
    @JsonProperty(CCLOUD_CONFIG_FIELD_NAME) CCloudConfig ccloudConfig,
    @Schema(description = "The details for connecting to Confluent Local.")
    @JsonProperty(LOCAL_CONFIG_FIELD_NAME) LocalConfig localConfig,
    @Schema(description = "The details for connecting to a CCloud, Confluent Platform, or "
                          + "Apache Kafka cluster.")
    @JsonProperty(KAFKA_CLUSTER_CONFIG_FIELD_NAME) KafkaClusterConfig kafkaClusterConfig,
    @Schema(description = "The details for connecting to a Schema Registry.")
    @JsonProperty(SCHEMA_REGISTRY_CONFIG_FIELD_NAME) SchemaRegistryConfig schemaRegistryConfig
) {

  public static final String CCLOUD_CONFIG_FIELD_NAME = "ccloud_config";
  public static final String LOCAL_CONFIG_FIELD_NAME = "local_config";
  public static final String KAFKA_CLUSTER_CONFIG_FIELD_NAME = "kafka_cluster";
  public static final String SCHEMA_REGISTRY_CONFIG_FIELD_NAME = "schema_registry";

  public enum ConnectionType {
    @Schema(description = "Connection type when using Confluent Local.")
    LOCAL,
    @Schema(description = "Connection type when using Confluent Platform to connect "
                          + "to clusters registered with MDS.")
    PLATFORM,
    @Schema(description = "Connection type when using Confluent Cloud and its available resources.")
    CCLOUD,
    @Schema(description = "Connection type when directly connecting to clusters and services.")
    DIRECT
  }

  public static ConnectionSpec createCCloud(String id, String name, CCloudConfig ccloudConfig) {
    return new ConnectionSpec(
        id,
        name,
        CCLOUD,
        ccloudConfig,
        null,
        null,
        null
    );
  }

  public static ConnectionSpec createLocal(String id, String name, LocalConfig localConfig) {
    return new ConnectionSpec(
        id,
        name,
        LOCAL,
        null,
        localConfig != null ? localConfig : new LocalConfig(null),
        null,
        null
    );
  }

  public static ConnectionSpec createDirect(
      String id, String name,
      KafkaClusterConfig kafkaConfig,
      SchemaRegistryConfig srConfig
  ) {
    return new ConnectionSpec(
        id,
        name,
        DIRECT,
        null,
        null,
        kafkaConfig,
        srConfig
    );
  }

  public ConnectionSpec(String id, String name, ConnectionType type) {
    this(id, name, type, null, null, null, null);
  }

  public ConnectionSpec withId(String id) {
    return new ConnectionSpec(
        id,
        name,
        type,
        ccloudConfig,
        localConfig,
        kafkaClusterConfig,
        schemaRegistryConfig
    );
  }

  public ConnectionSpec withName(String name) {
    return new ConnectionSpec(
        id,
        name,
        type,
        ccloudConfig,
        localConfig,
        kafkaClusterConfig,
        schemaRegistryConfig
    );
  }

  /**
   * Convenience method to return a new ConnectionSpec with the provided
   * Confluent Local configuration using an optional SR URI.
   *
   * @param srUri the URI of the local Schema Registry, or null if not used
   */
  public ConnectionSpec withLocalConfig(String srUri) {
    return new ConnectionSpec(
        id,
        name,
        type,
        ccloudConfig,
        new LocalConfig(srUri),
        kafkaClusterConfig,
        schemaRegistryConfig
    );
  }

  /**
   * Convenience method to return a new ConnectionSpec without the local config.
   */
  public ConnectionSpec withoutLocalConfig() {
    return new ConnectionSpec(
        id,
        name,
        type,
        ccloudConfig,
        null,
        kafkaClusterConfig,
        schemaRegistryConfig
    );
  }

  /**
   * Convenience method to return a new ConnectionSpec with the provided
   * Confluent Cloud organization ID set in the CCloudConfig.
   *
   * @param ccloudOrganizationId the Confluent Cloud organization ID to use; may be null
   */
  public ConnectionSpec withCCloudOrganizationId(String ccloudOrganizationId) {
    return new ConnectionSpec(
        id,
        name,
        type,
        new CCloudConfig(ccloudOrganizationId),
        localConfig,
        kafkaClusterConfig,
        schemaRegistryConfig
    );
  }

  /**
   * Convenience method to return a new ConnectionSpec with the provided
   * Kafka Cluster configuration.
   *
   * @param kafkaClusterConfig the Kafka cluster configuration; may be null
   */
  public ConnectionSpec withKafkaCluster(KafkaClusterConfig kafkaClusterConfig) {
    return new ConnectionSpec(
        id,
        name,
        type,
        ccloudConfig,
        localConfig,
        kafkaClusterConfig,
        schemaRegistryConfig
    );
  }

  /**
   * Convenience method to return a new ConnectionSpec with the provided
   * Schema Registry configuration.
   *
   * @param schemaRegistryConfig the Schema Registry configuration; may be null
   */
  public ConnectionSpec withSchemaRegistry(SchemaRegistryConfig schemaRegistryConfig) {
    return new ConnectionSpec(
        id,
        name,
        type,
        ccloudConfig,
        localConfig,
        kafkaClusterConfig,
        schemaRegistryConfig
    );
  }

  public String ccloudOrganizationId() {
    return ccloudConfig != null ? ccloudConfig.organizationId() : null;
  }

  @Schema(description = "Configuration for Confluent Cloud connections")
  public record CCloudConfig(
      @Schema(
          description = "The identifier of the CCloud organization to use. "
                        + "The user's default organization is used when absent."
      )
      @JsonProperty(value = "organization_id", required = true)
      @Size(min = 36, max = 36)
      String organizationId
  ) {
  }

  @Schema(description = "Configuration when using Confluent Local and "
                        + "optionally a local Schema Registry.")
  public record LocalConfig(
      @Schema(description = "The URL of the Schema Registry running locally.")
      @JsonProperty(value = "schema-registry-uri")
      @Null
      @Size(max = 512)
      String schemaRegistryUri
  ) {

    public void validate(
        List<Error> errors,
        String path,
        String what
    ) {
      // Note that when the SR URI is blank, we assume the user does not want to use SR.
      // When the SR URI is null, the user wants to use the SR at the default localhost & port
      if (schemaRegistryUri != null
          && !schemaRegistryUri.isEmpty()
          && schemaRegistryUri.isBlank()
      ) {
        // It has non-zero whitespace only, so this is invalid
        errors.add(
            Error.create()
                 .withDetail(
                     "Schema Registry URI may be null (use default local SR) or empty "
                     + "(do not use SR), but may not have only whitespace"
                 )
                 .withSource("%s.schema-registry-uri", path)
        );
      }
    }
  }

  @Schema(enumeration = {"PLAINTEXT", "SASL_SSL", "SASL_PLAINTEXT", "SSL"})
  @JsonDeserialize(using = SecurityProtocol.Deserializer.class)
  @RegisterForReflection
  public enum SecurityProtocol {
    @Schema(description = "Un-authenticated, non-encrypted channel")
    PLAINTEXT,
    @Schema(description = "SASL authenticated, non-encrypted channel")
    SASL_SSL,
    @Schema(description = "SASL authenticated, SSL channel")
    SASL_PLAINTEXT,
    @Schema(description = "SSL channel for mutual TLS")
    SSL,
    @Schema(hidden = true)
    @JsonEnumDefaultValue
    UNKNOWN;

    public boolean isSslEnabled() {
      return this == SSL || this == SASL_SSL;
    }

    /**
     * A custom deserializer to handle the security protocol literals that cannot be parsed.
     */
    public static class Deserializer extends JsonDeserializer<SecurityProtocol> {
      @Override
      public SecurityProtocol deserialize(
          JsonParser p,
          DeserializationContext ctxt
      ) throws IOException {
        try {
          return SecurityProtocol.valueOf(p.getValueAsString());
        } catch (IllegalArgumentException e) {
          return SecurityProtocol.UNKNOWN; // Return default on unknown value
        }
      }
    }

    /**
     * Get the list of allowed values for this enum, without hidden values.
     * @return the non-hidden allowed values as a comma-separated string
     */
    public static String allowedValues() {
      return Arrays
          .stream(values())
          .filter(v -> v != UNKNOWN)
          .map(Enum::name)
          .collect(Collectors.joining(", "));
    }
  }

  @Schema(enumeration = {"HTTPS", "NONE"})
  @JsonDeserialize(using = SslIdentificationAlgorithm.Deserializer.class)
  @RegisterForReflection
  public enum SslIdentificationAlgorithm {
    @Schema(
        description = "Perform broker hostname verification to prevent man-in-the-middle attacks"
    )
    HTTPS("https"),
    @Schema(description = "No broker hostname verification to allow self-signed certificates")
    NONE(""),
    @Schema(hidden = true)
    @JsonEnumDefaultValue
    UNKNOWN("unknown");

    private final String literal;

    SslIdentificationAlgorithm(String literal) {
      this.literal = literal;
    }

    @Override
    public String toString() {
      return literal;
    }

    /**
     * A custom deserializer to handle the security protocol literals that cannot be parsed.
     */
    public static class Deserializer extends JsonDeserializer<SslIdentificationAlgorithm> {
      @Override
      public SslIdentificationAlgorithm deserialize(
          JsonParser p,
          DeserializationContext ctxt
      ) throws IOException {
        try {
          return SslIdentificationAlgorithm.valueOf(p.getValueAsString());
        } catch (IllegalArgumentException e) {
          return SslIdentificationAlgorithm.UNKNOWN; // Return default on unknown value
        }
      }
    }

    /**
     * Get the list of allowed values for this enum, without hidden values.
     * @return the non-hidden allowed values as a comma-separated string
     */
    public static String allowedValues() {
      return Arrays
          .stream(values())
          .filter(v -> v != UNKNOWN)
          .map(Enum::name)
          .collect(Collectors.joining(", "));
    }
  }

  @Schema(description = "Kafka cluster configuration.")
  @RegisterForReflection
  public record KafkaClusterConfig(
      @Schema(description = "The identifier of the Kafka cluster, if known.")
      @Null
      @Size(max = ID_MAX_LEN)
      String id,

      @Schema(description = "A list of host/port pairs to use for establishing the "
                            + "initial connection to the Kafka cluster.")
      @JsonProperty(value = "bootstrap_servers")
      @Size(min = 1, max = BOOTSTRAP_SERVERS_MAX_LEN)
      @NotNull
      String bootstrapServers,

      @Schema(
          description =
              "The credentials for the Kafka cluster, or null if no authentication is required",
          oneOf = {
              BasicCredentials.class,
              ApiKeyAndSecret.class,
          },
          nullable = true
      )
      @Null
      Credentials credentials,

      @Schema(description = "The security protocol to use when connecting to the Kafka cluster.")
      @JsonProperty(value = "security_protocol")
      @NotNull
      SecurityProtocol securityProtocol,

      @Schema(
          description = "Whether to perform broker hostname verification when using SSL.",
          defaultValue = "HTTPS"
      )
      @JsonProperty(value = "ssl_identification_algorithm")
      @NotNull
      SslIdentificationAlgorithm sslIdentificationAlgorithm
  ) {

    private static final int ID_MAX_LEN = 64;
    private static final int BOOTSTRAP_SERVERS_MAX_LEN = 256;

    @JsonIgnore
    public Optional<KafkaEndpoint> asCCloudEndpoint() {
      return KafkaEndpoint.fromKafkaBootstrap(bootstrapServers());
    }

    public void validate(
        List<Error> errors,
        String path,
        String what
    ) {
      if (id != null && id.length() > ID_MAX_LEN) {
        errors.add(
            Error.create()
                 .withDetail("%s cluster ID may not be longer than %d characters", what, ID_MAX_LEN)
                 .withSource("%s.id", path)
        );
      }
      if (bootstrapServers == null || bootstrapServers.isBlank()) {
        errors.add(
            Error.create()
                 .withDetail("%s bootstrap_servers is required and may not be blank", what)
                 .withSource("%s.bootstrap_servers", path)
        );
      } else if (bootstrapServers.length() > BOOTSTRAP_SERVERS_MAX_LEN) {
        errors.add(
            Error.create()
                 .withDetail(
                     "%s bootstrap_servers must be at most %d characters",
                     what, BOOTSTRAP_SERVERS_MAX_LEN
                 )
                 .withSource("%s.bootstrap_servers", path)
        );
      }
      if (credentials != null) {
        credentials.validate(errors, "%s.credentials".formatted(path), what);
      }
      if (securityProtocol == SecurityProtocol.UNKNOWN) {
        var values = SecurityProtocol.allowedValues();
        errors.add(
            Error.create()
                 .withDetail("%s security protocol if provided must be one of: %s", what, values)
                 .withSource("%s.security_protocol", path)
        );
      }
      if (sslIdentificationAlgorithm == SslIdentificationAlgorithm.UNKNOWN) {
        var values = SslIdentificationAlgorithm.allowedValues();
        errors.add(
            Error.create()
                 .withDetail(
                     "%s SSL identification algorithm if provided must be one of: %s",
                     what,
                     values
                 )
                 .withSource("%s.ssl_identification_algorithm", path)
        );
      }
    }
  }

  @Schema(description = "Schema Registry configuration.")
  @RegisterForReflection
  public record SchemaRegistryConfig(
      @Schema(description = "The identifier of the Schema Registry cluster, if known.")
      @Null
      @Size(max = ID_MAX_LEN)
      String id,

      @Schema(description = "The URL of the Schema Registry.")
      @JsonProperty(value = "uri")
      @Size(min = 1, max = URI_MAX_LEN)
      @NotNull
      String uri,

      @Schema(
          description = "The credentials for the Schema Registry, or null if "
                        + "no authentication is required",
          oneOf = {
              BasicCredentials.class,
              ApiKeyAndSecret.class,
          },
          nullable = true
      )
      @Null
      Credentials credentials
  ) {
    private static final int ID_MAX_LEN = 64;
    private static final int URI_MAX_LEN = 256;

    @JsonIgnore
    public Optional<SchemaRegistryEndpoint> asCCloudEndpoint() {
      return SchemaRegistryEndpoint.fromUri(uri());
    }

    public void validate(
        List<Error> errors,
        String path,
        String what
    ) {
      if (id != null && id.length() > 64) {
        errors.add(
            Error.create()
                 .withDetail("%s cluster ID may not be longer than %d characters", what, 64)
                 .withSource("%s.id", path)
        );
      }
      if (uri == null || uri.isBlank()) {
        errors.add(
            Failure.Error.create()
                         .withDetail("%s URI is required and may not be blank", what)
                         .withSource("%s.uri", path)
        );
      } else if (uri.length() > URI_MAX_LEN) {
        errors.add(
            Failure.Error.create()
                         .withDetail("%s URI must be at most %d characters", what, URI_MAX_LEN)
                         .withSource("%s.uri", path)
        );
      } else {
        try {
          new URI(uri);
        } catch (URISyntaxException e) {
          errors.add(
              Failure.Error.create()
                           .withDetail("%s URI is not a valid URI", what)
                           .withSource("%s.uri", path)
          );
        }
      }
      if (credentials != null) {
        credentials.validate(errors, "%s.credentials".formatted(path), what);
      }
    }
  }

  /**
   * Validate that this ConnectionSpec is structurally valid.
   * The spec may still have missing or incomplete fields, but it should be structurally sound.
   */
  public List<Error> validate() {
    return validateUpdate(this);
  }

  /**
   * Validate that the provided ConnectionSpec is a valid update from
   * the current ConnectionSpec.
   * The spec may still have missing or incomplete fields, but it should be structurally sound.
   */
  @SuppressWarnings({
      "CyclomaticComplexity",
      "NPathComplexity"
  })
  public List<Error> validateUpdate(ConnectionSpec newSpec) {
    var errors = new ArrayList<Error>();

    // Check required fields and immutability
    if (newSpec.name == null || newSpec.name.isBlank()) {
      checkRequired(errors, "name", "Connection name");
    }
    if (newSpec.id == null || newSpec.id.isBlank()) {
      checkRequired(errors, "id", "Connection ID");
    } else if (!Objects.equals(newSpec.id, id)) {
      checkImmutable(errors, "id", "Connection ID");
    }
    if (newSpec.type == null) {
      checkRequired(errors, "type", "Connection type");
    } else if (!Objects.equals(newSpec.type, type)) {
      checkImmutable(errors, "type", "Connection type");
    } else {
      // The type is the same, so we can check type-specific fields

      // Check type-specific fields
      switch (newSpec.type) {
        case LOCAL -> {
          checkCCloudConfigNotAllowed(errors, newSpec);
          checkKafkaClusterNotAllowed(errors, newSpec);
          // Allow use of the older local config with Schema Registry.
          var local = newSpec.localConfig;
          if (local != null) {
            local.validate(errors, "local_config", "Local configuration");
          }
          // But also support the new Schema Registry config
          var sr = newSpec.schemaRegistryConfig();
          if (sr != null) {
            sr.validate(errors, "schema_registry", "Schema Registry");
          }
          // Make sure we're not using both
          if (sr != null && local != null && local.schemaRegistryUri != null) {
            errors.add(
                Error.create()
                     .withDetail("Local config cannot be used with schema_registry configuration")
                     .withSource("local_config.schema-registry-uri")
            );
          }
        }
        case CCLOUD -> {
          checkLocalConfigNotAllowed(errors, newSpec);
          checkKafkaClusterNotAllowed(errors, newSpec);
          checkSchemaRegistryNotAllowed(errors, newSpec);
        }
        case DIRECT -> {
          var kafka = newSpec.kafkaClusterConfig();
          if (kafka != null) {
            kafka.validate(errors, "kafka_cluster", "Kafka cluster");
          }
          var sr = newSpec.schemaRegistryConfig();
          if (sr != null) {
            sr.validate(errors, "schema_registry", "Schema Registry");
          }
          checkLocalConfigNotAllowed(errors, newSpec);
          checkCCloudConfigNotAllowed(errors, newSpec);
        }
        case PLATFORM -> {
        }
        default -> {
          errors.add(
              Error.create()
                   .withDetail("Unknown connection type: %s".formatted(newSpec.type()))
                   .withSource("type")
          );
        }
      }
    }
    return errors;
  }

  void checkLocalConfigNotAllowed(List<Error> errors, ConnectionSpec newSpec) {
    if (newSpec.localConfig != null) {
      checkAllowedWhen(
          errors,
          LOCAL_CONFIG_FIELD_NAME,
          "Local configuration",
          "type is %s".formatted(newSpec.type)
      );
    }
  }

  void checkCCloudConfigNotAllowed(List<Error> errors, ConnectionSpec newSpec) {
    if (newSpec.ccloudConfig != null) {
      checkAllowedWhen(
          errors,
          CCLOUD_CONFIG_FIELD_NAME,
          "CCloud configuration",
          "type is %s".formatted(newSpec.type)
      );
    }
  }

  void checkKafkaClusterNotAllowed(List<Error> errors, ConnectionSpec newSpec) {
    if (newSpec.kafkaClusterConfig != null) {
      checkAllowedWhen(
          errors,
          KAFKA_CLUSTER_CONFIG_FIELD_NAME,
          "Kafka cluster configuration",
          "type is %s".formatted(newSpec.type)
      );
    }
  }

  void checkSchemaRegistryNotAllowed(List<Error> errors, ConnectionSpec newSpec) {
    if (newSpec.schemaRegistryConfig != null) {
      checkAllowedWhen(
          errors,
          SCHEMA_REGISTRY_CONFIG_FIELD_NAME,
          "Schema Registry configuration",
          "type is %s".formatted(newSpec.type)
      );
    }
  }

  void checkAllowedWhen(List<Error> errors, String path, String what, String when) {
    errors.add(
        Error.create()
             .withDetail("%s is not allowed when %s".formatted(what, when))
             .withSource(path)
    );
  }

  void checkRequired(List<Error> errors, String path, String what) {
    errors.add(
        Error.create()
             .withDetail("%s is required and may not be blank".formatted(what))
             .withSource(path)
    );
  }

  void checkImmutable(List<Error> errors, String path, String what) {
    errors.add(
        Error.create()
             .withDetail("%s may not be changed".formatted(what))
             .withSource(path)
    );
  }
}