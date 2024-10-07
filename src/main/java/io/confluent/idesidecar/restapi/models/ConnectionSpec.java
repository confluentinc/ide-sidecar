package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
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
    @JsonProperty("local_config") LocalConfig localConfig,
    @JsonProperty("broker_config") BrokerConfig brokerConfig
) {

  public enum ConnectionType {
    LOCAL,
    PLATFORM,
    CCLOUD
  }

  public ConnectionSpec(String id, String name, ConnectionType type) {
    this(id, name, type, null, null, null);
  }

  public ConnectionSpec withId(String id) {
    return new ConnectionSpec(id, name, type, ccloudConfig, localConfig, brokerConfig);
  }

  public ConnectionSpec withName(String name) {
    return new ConnectionSpec(id, name, type, ccloudConfig, localConfig, brokerConfig);
  }

  /**
   * Convenience method to return a new ConnectionSpec with the provided
   * Confluent Cloud organization ID set in the CCloudConfig.
   */
  public ConnectionSpec withCCloudOrganizationId(String ccloudOrganizationId) {
    return new ConnectionSpec(
        id,
        name,
        type,
        new CCloudConfig(ccloudOrganizationId),
        localConfig,
        brokerConfig
    );
  }

  public String ccloudOrganizationId() {
    return ccloudConfig != null ? ccloudConfig.organizationId() : null;
  }

  public String bootstrapServers() {
    return brokerConfig != null ? brokerConfig.bootstrapServers() : null;
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

  @Schema(description = "Configuration for broker")
  public record BrokerConfig(
      // Descriptions from https://kafka.apache.org/documentation/
      @JsonPropertyDescription("""
          A list of host/port pairs to use for establishing the initial connection to the
          Kafka cluster. The client will make use of all servers irrespective of which servers are
          specified here for bootstrapping—this list only impacts the initial hosts used to discover
          the full set of servers. This list should be in the form host1:port1,host2:port2,....
          Since these servers are just used for the initial connection to discover the full cluster
          membership (which may change dynamically), this list need not contain the full set of
          servers (you may want more than one, though, in case a server is down).
          """)
      @JsonProperty(value = "bootstrap_servers", required = true)
      String bootstrapServers
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