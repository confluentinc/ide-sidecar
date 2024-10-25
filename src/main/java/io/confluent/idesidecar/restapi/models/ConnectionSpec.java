package io.confluent.idesidecar.restapi.models;

import static io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType.CCLOUD;
import static io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType.DIRECT;
import static io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType.LOCAL;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.confluent.idesidecar.restapi.util.CCloud.KafkaEndpoint;
import io.confluent.idesidecar.restapi.util.CCloud.SchemaRegistryEndpoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import javax.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "The connection details that can be set or changed.")
@JsonInclude(JsonInclude.Include.NON_NULL)
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
  }

  @Schema(description = "Kafka cluster configuration.")
  public record KafkaClusterConfig(
      @Schema(description = "The identifier of the Kafka cluster, if known.")
      @Null
      @Size(max = 64)
      String id,

      @Schema(description = "A list of host/port pairs to use for establishing the "
                            + "initial connection to the Kafka cluster.")
      @JsonProperty(value = "bootstrap_servers")
      @Size(min = 1, max = 256)
      @NotNull
      String bootstrapServers
  ) {

    @JsonIgnore
    public Optional<KafkaEndpoint> asCCloudEndpoint() {
      return KafkaEndpoint.fromKafkaBootstrap(bootstrapServers());
    }
  }

  @Schema(description = "Schema Registry configuration.")
  public record SchemaRegistryConfig(
      @Schema(description = "The identifier of the Schema Registry cluster, if known.")
      @Null
      @Size(max = 64)
      String id,

      @Schema(description = "The URL of the Schema Registry.")
      @JsonProperty(value = "uri")
      @Size(min = 1, max = 256)
      @NotNull
      String uri
  ) {

    @JsonIgnore
    public Optional<SchemaRegistryEndpoint> asCCloudEndpoint() {
      return SchemaRegistryEndpoint.fromUri(uri());
    }
  }

  @Schema(description = "Basic authentication credentials")
  public record BasicCredentials(
      @Schema(description = "The username to use when connecting to the external service.")
      @JsonProperty(value = "username")
      @Size(max = 64)
      @NotNull
      String username,

      @Schema(description = "The password to use when connecting to the external service.")
      @JsonProperty(value = "password")
      @Size(max = 64)
      @NotNull
      // TODO: Wrap in Secret record rather than String, and override toString to
      //  prevent/limit read access
      String password
  ) {

    // TODO: This shouldn't be needed once we define a Secret record, which won't have toString()
    @Override
    public String toString() {
      // Do not print the username, in case this object is logged
      return "BasicCredentials{username='%s', password=********}".formatted(username);
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
            // Note that when the SR URI is blank, we assume the user does not want to use SR.
            // When the SR URI is null, the user wants to use the SR at the default localhost & port
            var uri = local.schemaRegistryUri;
            if (uri != null && !uri.isEmpty() && uri.trim().isEmpty()) {
              // It has non-zero whitespace only, so this is invalid
              errors.add(
                  Error.create()
                       .withDetail(
                           "Schema Registry URI may null (use default local SR) or empty "
                           + "(do not use SR), but may not have only whitespace"
                       )
                       .withSource("local_config.schema-registry-uri")
              );
            }
          }
          // But also support the new Schema Registry config
          var sr = newSpec.schemaRegistryConfig();
          if (sr != null) {
            if (sr.uri == null || sr.uri.isBlank()) {
              checkRequired(errors, "schema_registry.uri", "Schema Registry URI");
            }
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
            if (kafka.bootstrapServers == null || kafka.bootstrapServers.isBlank()) {
              checkRequired(
                  errors,
                  "kafka_cluster.bootstrap_servers",
                  "Kafka cluster bootstrap_servers"
              );
            }
          }
          var sr = newSpec.schemaRegistryConfig();
          if (sr != null) {
            if (sr.uri == null || sr.uri.isBlank()) {
              checkRequired(errors, "schema_registry.uri", "Schema Registry URI");
            }
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