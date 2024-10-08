package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ConnectionSpec(
    String id,
    String name,
    ConnectionType type,
    @JsonProperty("ccloud_config") CCloudConfig ccloudConfig,
    @JsonProperty("local_config") LocalConfig localConfig
) {

  public enum ConnectionType {
    LOCAL,
    PLATFORM,
    CCLOUD
  }

  public ConnectionSpec(String id, String name, ConnectionType type) {
    this(id, name, type, null, null);
  }

  public ConnectionSpec withId(String id) {
    return new ConnectionSpec(id, name, type, ccloudConfig, localConfig);
  }

  public ConnectionSpec withName(String name) {
    return new ConnectionSpec(id, name, type, ccloudConfig, localConfig);
  }

  /**
   * Convenience method to return a new ConnectionSpec with the provided
   * Confluent Cloud organization ID set in the CCloudConfig.
   */
  public ConnectionSpec withCCloudOrganizationId(String ccloudOrganizationId) {
    return new ConnectionSpec(id, name, type, new CCloudConfig(ccloudOrganizationId), localConfig);
  }

  public String ccloudOrganizationId() {
    return ccloudConfig != null ? ccloudConfig.organizationId() : null;
  }

  @Schema(description = "Configuration for Confluent Cloud connections")
  public record CCloudConfig(
      @JsonProperty(value = "organization_id", required = true) String organizationId
  ) {

  }

  @Schema(description = "Configuration for local cluster")
  public record LocalConfig(
      @JsonProperty(value = "schema-registry-uri") String schemaRegistryUri
  ) {
  }

  /**
   * Validate that the provided ConnectionSpec is a valid update from
   * the current ConnectionSpec.
   */
  public List<Error> validateUpdate(ConnectionSpec newSpec) {
    var errors = new ArrayList<Error>();
    if (newSpec.name() == null || newSpec.name().isEmpty()) {
      errors.add(Error.create().withDetail("Connection name cannot be null or empty")
          .withSource("name"));
    }
    if (!Objects.equals(newSpec.id(), id)) {
      errors.add(Error.create().withDetail("Connection ID cannot be changed").withSource("id"));
    }
    if (!Objects.equals(newSpec.type(), type)) {
      errors.add(Error.create().withDetail("Connection type cannot be changed").withSource("type"));
    }
    if (newSpec.ccloudConfig() != null && newSpec.type() != ConnectionType.CCLOUD) {
      errors.add(Error.create().withDetail("CCloud config cannot be set for non-CCloud connections")
          .withSource("ccloud_config"));
    }
    return errors;
  }
}