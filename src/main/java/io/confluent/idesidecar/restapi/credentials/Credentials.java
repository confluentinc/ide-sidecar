package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.soabase.recordbuilder.core.RecordBuilder;
import io.vertx.core.MultiMap;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Base interface for credentials objects used with Kafka and Schema Registry clients.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({
    @Type(value = BasicCredentials.class),
    @Type(value = ApiKeyAndSecret.class),
    @Type(value = OAuthCredentials.class)
})
@RegisterForReflection
public interface Credentials {

  @RecordBuilder
  record KafkaConnectionOptions(
      boolean redact,
      TLSConfig tlsConfig
  ) implements CredentialsKafkaConnectionOptionsBuilder.With {
  }

  @RecordBuilder
  record SchemaRegistryConnectionOptions(
      boolean redact,
      TLSConfig tlsConfig
  ) implements CredentialsSchemaRegistryConnectionOptionsBuilder.With {
  }

  enum Type {
    BASIC,
    MUTUAL_TLS,
    OAUTH2,
    API_KEY_AND_SECRET,
  }

  /**
   * Get the type of credentials. This is not included in the JSON serialized representations.
   *
   * @return the type
   */
  @JsonIgnore
  @NotNull
  Type type();

  /**
   * Return true if this is a basic credentials object.
   *
   * @return true if {@link #type()} equals {@link Type#BASIC}
   */
  @JsonIgnore
  default boolean isBasic() {
    return type() == Type.BASIC;
  }

  /**
   * Return true if this is an OAuth 2.0 credentials object.
   *
   * @return true if {@link #type()} equals {@link Type#OAUTH2}
   */
  @JsonIgnore
  default boolean isOauth2() {
    return type() == Type.OAUTH2;
  }

  /**
   * Return true if this is an API key and secret credentials object.
   *
   * @return true if {@link #type()} equals {@link Type#API_KEY_AND_SECRET}
   */
  @JsonIgnore
  default boolean isApiKeyAndSecret() {
    return type() == Type.API_KEY_AND_SECRET;
  }

  /**
   * Get the Kafka client authentication-related properties for this credentials object.
   *
   * @param options the connection options
   * @return the authentication-related Kafka client properties, or empty if these credentials
   *         cannot be used with Kafka clients
   */
  @JsonIgnore
  default Optional<Map<String, String>> kafkaClientProperties(
      KafkaConnectionOptions options
  ) {
    return Optional.empty();
  }

  /**
   * Get the Schema Registry client authentication-related properties for this credentials object.
   *
   * @param options the connection options
   * @return the authentication-related SR client properties, or empty if these credentials
   *         cannot be used with SR clients
   */
  @JsonIgnore
  default Optional<Map<String, String>> schemaRegistryClientProperties(
      SchemaRegistryConnectionOptions options
  ) {
    return Optional.empty();
  }

  /**
   * Create the header(s) for an HTTP client connection. This is needed for connecting and
   * authenticating to a Kafka REST proxy, Schema Registry, or MDS over HTTP.
   *
   * @return the authentication-related HTTP client headers, or empty if these credentials
   *         cannot be used with HTTP client properties
   */
  default Optional<MultiMap> httpClientHeaders() {
    return Optional.empty();
  }

  void validate(List<Failure.Error> errors, String path, String what);
}
