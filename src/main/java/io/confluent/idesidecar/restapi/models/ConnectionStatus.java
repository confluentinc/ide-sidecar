package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.auth.AuthErrors;
import io.soabase.recordbuilder.core.RecordBuilder;
import jakarta.validation.constraints.Null;
import java.time.Instant;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Represents the status of a connection.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@RecordBuilder
public record ConnectionStatus(
    @JsonProperty("ccloud")
    CCloudStatus ccloud,

    @JsonProperty("kafka_cluster")
    KafkaClusterStatus kafkaCluster,

    @JsonProperty("schema_registry")
    SchemaRegistryStatus schemaRegistry
) implements ConnectionStatusBuilder.With {

  /**
   * Return whether the supplied connection has been successfully established with some or all
   * components.
   *
   * @return true if the connection has been established for at least one component, or false
   * otherwise
   */
  @JsonIgnore
  public boolean isConnected() {
    return ccloud != null && ccloud.isConnected()
        || kafkaCluster != null && kafkaCluster.isConnected()
        || schemaRegistry != null && schemaRegistry.isConnected();
  }

  public interface StateOwner {

    ConnectedState state();

    @JsonIgnore
    default boolean isConnected() {
      return state() == ConnectedState.SUCCESS;
    }
  }

  /**
   * Initial status of any connection. A new connection does not hold any token, does not know the
   * end of the lifetime of its tokens, and has not faced any errors.
   */
  public static final ConnectionStatus INITIAL_STATUS =
      new ConnectionStatus(
          null,
          null,
          null
      );

  public static final ConnectionStatus INITIAL_CCLOUD_STATUS =
      new ConnectionStatus(
          new CCloudStatus(ConnectedState.NONE, null, null, null),
          null,
          null
      );

  public enum ConnectedState {
    @Schema(description = "No connection has been established yet.")
    NONE,
    @Schema(description = "Currently attempting to establish the connection.")
    ATTEMPTING,
    @Schema(description = "The connection has been successfully established.")
    SUCCESS,
    @Schema(description = "The connection has expired and must be re-established.")
    EXPIRED,
    @Schema(description = "The connection has failed.")
    FAILED
  }

  @Schema(description = "The status related to CCloud.")
  @JsonInclude(Include.NON_NULL)
  @RecordBuilder
  public record CCloudStatus(
      @Schema(
          description = "The state of the connection to CCloud."
      )
      @JsonProperty(required = true)
      ConnectedState state,

      @Schema(description =
          "If the connection's auth context holds a valid token, this attribute holds the time at "
              + "which the user must re-authenticate because, for instance, the refresh token reached "
              + "the end of its absolute lifetime."
      )
      @JsonProperty(value = "requires_authentication_at")
      Instant requiresAuthenticationAt,

      @Schema(description = "Information about the authenticated principal, if known.")
      @JsonProperty
      @Null
      UserInfo user,

      @Schema(description = "Errors related to the connection to the Kafka cluster.")
      @JsonProperty
      @Null
      AuthErrors errors
  ) implements StateOwner, ConnectionStatusCCloudStatusBuilder.With {

  }

  @Schema(description = "The status related to the specified Kafka cluster.")
  @JsonInclude(Include.NON_NULL)
  @RecordBuilder
  public record KafkaClusterStatus(
      @Schema(description = "The state of the connection to the Kafka cluster.")
      @JsonProperty(required = true)
      ConnectedState state,

      @Schema(description = "Information about the authenticated principal, if known.")
      @JsonProperty
      @Null
      UserInfo user,

      @Schema(description = "Errors related to the connection to the Kafka cluster.")
      @JsonProperty
      @Null
      AuthErrors errors
  ) implements StateOwner, ConnectionStatusKafkaClusterStatusBuilder.With {

  }

  @Schema(description = "The status related to the specified Schema Registry.")
  @JsonInclude(Include.NON_NULL)
  @RecordBuilder
  public record SchemaRegistryStatus(
      @Schema(description = "The state of the connection to the Schema Registry.")
      @JsonProperty(required = true)
      ConnectedState state,

      @Schema(description = "Information about the authenticated principal, if known.")
      @JsonProperty
      @Null
      UserInfo user,

      @Schema(description = "Errors related to the connection to the Schema Registry.")
      @JsonProperty
      @Null
      AuthErrors errors
  ) implements StateOwner, ConnectionStatusSchemaRegistryStatusBuilder.With {

  }

  /**
   * The information known about the user of this connection. Any of these values can be null
   *
   * @param id        the system-specific identifier for the user, if different than the username
   * @param username  the username or email address
   * @param firstName the user's first name, if known
   * @param lastName  the user's last name, if known
   */
  @JsonInclude(Include.NON_NULL)
  public record UserInfo(

      String id,
      String username,
      @JsonProperty(value = "first_name") String firstName,
      @JsonProperty(value = "last_name") String lastName,
      @JsonProperty(value = "social_connection") String socialConnection,
      @JsonProperty(value = "auth_type") String authType
  ) {

  }
}