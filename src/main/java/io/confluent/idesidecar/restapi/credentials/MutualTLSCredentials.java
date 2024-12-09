package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Null;
import jakarta.validation.constraints.Size;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Mutual TLS authentication credentials")
public record MutualTLSCredentials(
    @Schema(description = "The path to the local trust store file.")
    @JsonProperty(value = "truststore_path")
    @Size(max = TRUSTSTORE_PATH_MAX_LEN)
    @NotNull
    String truststorePath,

    @Schema(
        description = "The password for the local trust store file. If a password is not set, "
                      + "trust store file configured will still be used, but integrity checking "
                      + "is disabled. A trust store password is not supported for PEM format."
    )
    @JsonProperty(value = "truststore_password")
    @Size(max = TRUSTSTORE_PASSWORD_MAX_LEN)
    @Null
    Password truststorePassword,

    @Schema(description = "The file format of the local trust store file")
    @JsonProperty(value = "truststore_type")
    @Null
    StoreType truststoreType,

    @Schema(description = "The path to the local key store file.")
    @JsonProperty(value = "keystore_path")
    @Size(max = KEYSTORE_PATH_MAX_LEN)
    @NotNull
    String keystorePath,

    @Schema(
        description = "The password for the local key store file. If a password is not set, trust "
                      + " store file configured will still be used, but integrity checking is "
                      + "disabled. A key store password is not supported for PEM format."
    )
    @JsonProperty(value = "keystore_password")
    @Size(max = KEYSTORE_PASSWORD_MAX_LEN)
    @Null
    Password keystorePassword,

    @Schema(description = "The file format of the local key store file.")
    @JsonProperty(value = "keystore_type")
    @Null
    StoreType keystoreType,

    @Schema(description = "The password of the private key in the local key store file.")
    @JsonProperty(value = "key_password")
    @Size(max = KEY_PASSWORD_MAX_LEN)
    @Null
    Password keyPassword
) implements Credentials {

  private static final int TRUSTSTORE_PATH_MAX_LEN = 256;
  private static final int TRUSTSTORE_PASSWORD_MAX_LEN = 256;
  private static final int KEYSTORE_PATH_MAX_LEN = 256;
  private static final int KEYSTORE_PASSWORD_MAX_LEN = 256;
  private static final int KEY_PASSWORD_MAX_LEN = 256;

  @JsonDeserialize(using = StoreType.Deserializer.class)
  public enum StoreType {
    JKS,
    PKCS12,
    PEM,
    @Schema(hidden = true)
    @JsonEnumDefaultValue
    UNKNOWN;

    /**
     * A custom deserializer to handle the store type literals that cannot be parsed.
     */
    public static class Deserializer extends JsonDeserializer<StoreType> {
      @Override
      public StoreType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        try {
          return StoreType.valueOf(p.getValueAsString());
        } catch (IllegalArgumentException e) {
          return StoreType.UNKNOWN; // Return default on unknown value
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

  public MutualTLSCredentials(
      String truststorePath,
      Password truststorePassword,
      String keystorePath,
      Password keystorePassword,
      Password keyPassword
  ) {
    this(
        truststorePath,
        truststorePassword,
        null,
        keystorePath,
        keystorePassword,
        null,
        keyPassword
    );
  }

  @Override
  public Type type() {
    return Type.MUTUAL_TLS;
  }

  @Override
  public Optional<Map<String, String>> kafkaClientProperties(
      KafkaConnectionOptions options
  ) {
    var config = new LinkedHashMap<String, String>();
    config.put("security.protocol", "SSL");
    if (!options.verifyCertificates()) {
      config.put("ssl.endpoint.identification.algorithm", "");
    }
    config.put("ssl.truststore.location", truststorePath);
    if (truststoreType != null && truststoreType != StoreType.UNKNOWN) {
      config.put("ssl.truststore.type", truststoreType.name());
    }
    if (truststorePassword != null) {
      config.put("ssl.truststore.password", truststorePassword.asString(options.redact()));
    }
    config.put("ssl.keystore.location", keystorePath);
    if (keystoreType != null && keystoreType != StoreType.UNKNOWN) {
      config.put("ssl.keystore.type", keystoreType.name());
    }
    if (keystorePassword != null) {
      config.put("ssl.keystore.password", keystorePassword.asString(options.redact()));
    }
    if (keyPassword != null) {
      config.put("ssl.key.password", keyPassword.asString(options.redact()));
    }
    return Optional.of(config);
  }

  /**
   * Schema Registry appears to support mTLS authentication with
   * <a href="https://github.com/confluentinc/schema-registry/pull/957">this PR</a>,
   * which basically uses the same properties and mechanisms as the Kafka clients except with
   * a {@code schema.registry.} prefix. The documentation is unclear.
   *
   * @param options the connection options
   * @return an empty optional
   */
  @Override
  public Optional<Map<String, String>> schemaRegistryClientProperties(
      SchemaRegistryConnectionOptions options
  ) {
    var config = new LinkedHashMap<String, String>();
    config.put("ssl.truststore.location", truststorePath);

    // TODO: Guard this behind options.verifyCertificates()
    config.put("ssl.endpoint.identification.algorithm", "");

    if (truststoreType != null && truststoreType != StoreType.UNKNOWN) {
      config.put("ssl.truststore.type", truststoreType.name());
    }
    if (truststorePassword != null) {
      config.put("ssl.truststore.password", truststorePassword.asString(options.redact()));
    }
    config.put("ssl.keystore.location", keystorePath);
    if (keystoreType != null && keystoreType != StoreType.UNKNOWN) {
      config.put("ssl.keystore.type", keystoreType.name());
    }
    if (keystorePassword != null) {
      config.put("ssl.keystore.password", keystorePassword.asString(options.redact()));
    }
    if (keyPassword != null) {
      config.put("ssl.key.password", keyPassword.asString(options.redact()));
    }
    return Optional.of(config);
  }

  @Override
  public void validate(
      List<Failure.Error> errors,
      String path,
      String what
  ) {
    if (truststoreType == StoreType.UNKNOWN) {
      var values = StoreType.allowedValues();
      errors.add(
          Error.create()
               .withDetail("%s truststore type if provided must be one of: %s", what, values)
               .withSource("%s.truststore_type", path)
      );
    }
    if (truststorePath == null || truststorePath.isBlank()) {
      errors.add(
          Error.create()
               .withDetail("%s truststore path is required and may not be blank", what)
               .withSource("%s.truststore_path", path)
      );
    } else if (truststorePath.length() > TRUSTSTORE_PATH_MAX_LEN) {
      errors.add(
          Error.create()
               .withDetail(
                   "%s truststore path may not be longer than %d characters",
                   what,
                   TRUSTSTORE_PATH_MAX_LEN
               )
               .withSource("%s.truststore_path", path)
      );
    }
    if (keystoreType == StoreType.UNKNOWN) {
      var values = StoreType.allowedValues();
      errors.add(
          Error.create()
               .withDetail("%s keystore type if provided must be one of: %s", what, values)
               .withSource("%s.keystore_type", path)
      );
    }
    if (keystorePath == null || keystorePath.isBlank()) {
      errors.add(
          Error.create()
               .withDetail("%s keystore path is required and may not be blank", what)
               .withSource("%s.keystore_path", path)
      );
    } else if (keystorePath.length() > KEYSTORE_PATH_MAX_LEN) {
      errors.add(
          Error.create()
               .withDetail(
                   "%s keystore path may not be longer than %d characters",
                   what,
                   KEYSTORE_PATH_MAX_LEN
               )
               .withSource("%s.keystore_path", path)
      );
    }
    if (truststorePassword != null && truststorePassword.longerThan(TRUSTSTORE_PASSWORD_MAX_LEN)) {
      errors.add(
          Error.create()
               .withDetail(
                   "%s truststore password may not be longer than %d characters",
                   what,
                   TRUSTSTORE_PASSWORD_MAX_LEN
               )
               .withSource("%s.truststore_password", path)
      );
    }
    if (keystorePassword != null && keystorePassword.longerThan(KEYSTORE_PASSWORD_MAX_LEN)) {
      errors.add(
          Error.create()
               .withDetail(
                   "%s keystore password may not be longer than %d characters",
                   what,
                   KEYSTORE_PASSWORD_MAX_LEN
               )
               .withSource("%s.keystore_password", path)
      );
    }
    if (keyPassword != null && keyPassword.longerThan(KEY_PASSWORD_MAX_LEN)) {
      errors.add(
          Error.create()
               .withDetail(
                   "%s key password may not be longer than %d characters",
                   what,
                   KEY_PASSWORD_MAX_LEN
               )
               .withSource("%s.key_password", path)
      );
    }
  }
}
