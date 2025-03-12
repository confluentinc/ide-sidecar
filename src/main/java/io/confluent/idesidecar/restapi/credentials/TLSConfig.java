package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.soabase.recordbuilder.core.RecordBuilder;
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

/**
 * The SSL/TLS configuration object. Usage modes:
 * <ul>
 *   <li>Default SSL settings without truststore or keystore: set {@link #enabled} to {@code true}
 *   and leave other fields unset</li>
 *   <li>Truststore only: provide a truststore path and password</li>
 *   <li>Truststore and keystore: provide a truststore path and password, and a keystore path and
 *   password. This is equivalent to mutual TLS or mTLS</li>
 *   <li>Disable hostname verification: set {@link #verifyHostname} to {@code false} to disable
 *   server certificate hostname verification</li>
 *   <li>SSL Disabled: set {@link #enabled} to {@code false} to disable SSL</li>
 * </ul>
 */
@Schema(description = "SSL configuration")
@RecordBuilder
public record TLSConfig(
    @Schema(
        description = "Whether to verify the server certificate hostname."
            + " Defaults to true if not set.",
        defaultValue = DEFAULT_VERIFY_HOSTNAME_VALUE
    )
    @JsonProperty(value = "verify_hostname")
    @Null
    Boolean verifyHostname,

    @Schema(
        description = "Whether SSL is enabled. If not set, defaults to true.",
        defaultValue = DEFAULT_ENABLED_VALUE,
        required = true
    )
    @JsonProperty(value = "enabled")
    @NotNull
    Boolean enabled,

    @Schema(
        description = "The trust store configuration for authenticating the server's certificate.",
        nullable = true
    )
    @JsonProperty(value = "truststore")
    @Null
    TrustStore truststore,

    @Schema(
        description = "The key store configuration that will identify and authenticate "
            + "the client to the server, required for mutual TLS (mTLS)",
        nullable = true
    )
    @JsonProperty(value = "keystore")
    @Null
    KeyStore keystore
) implements TLSConfigBuilder.With {

  @RecordBuilder
  public record TrustStore(
      @Schema(description = "The path to the local trust store file. Required for authenticating "
          + "the server's certificate.")
      @JsonProperty(value = "path")
      @Size(max = TRUSTSTORE_PATH_MAX_LEN)
      @NotNull
      String path,

      @Schema(
          description = "The password for the local trust store file. If a password is not set, "
              + "trust store file configured will still be used, but integrity checking "
              + "is disabled. A trust store password is not supported for PEM format.",
          nullable = true
      )
      @JsonProperty(value = "password")
      @Size(max = TRUSTSTORE_PASSWORD_MAX_LEN)
      @Null
      Password password,

      @Schema(description = "The file format of the local trust store file",
          defaultValue = DEFAULT_STORE_TYPE,
          nullable = true)
      @JsonProperty(value = "type")
      @Null
      StoreType type
  ) {

    public void validate(
        List<Failure.Error> errors,
        String path,
        String what
    ) {
      if (this.path == null || this.path.isBlank()) {
        errors.add(
            Error.create()
                .withDetail("%s truststore path is required and may not be blank", what)
                .withSource("%s.path", path)
        );
      } else if (this.path.length() > TRUSTSTORE_PATH_MAX_LEN) {
        errors.add(
            Error.create()
                .withDetail(
                    "%s truststore path may not be longer than %d characters",
                    what,
                    TRUSTSTORE_PATH_MAX_LEN
                )
                .withSource("%s.path", path)
        );
      }
      if (password != null && password.longerThan(TRUSTSTORE_PASSWORD_MAX_LEN)) {
        errors.add(
            Error.create()
                .withDetail(
                    "%s truststore password may not be longer than %d characters",
                    what,
                    TRUSTSTORE_PASSWORD_MAX_LEN
                )
                .withSource("%s.password", path)
        );
      }
    }
  }

  @RecordBuilder
  public record KeyStore(
      @Schema(description = "The path to the local key store file. Only specified if client "
          + "needs to be authenticated by the server (mutual TLS).")
      @JsonProperty(value = "path")
      @Size(max = KEYSTORE_PATH_MAX_LEN)
      @NotNull
      String path,

      @Schema(
          description =
              "The password for the local key store file. If a password is not set, key "
                  + "store file configured will still be used, but integrity checking is "
                  + "disabled. A key store password is not supported for PEM format.",
          nullable = true
      )
      @JsonProperty(value = "password")
      @Size(max = KEYSTORE_PASSWORD_MAX_LEN)
      @Null
      Password password,

      @Schema(description = "The file format of the local key store file.",
          defaultValue = DEFAULT_STORE_TYPE,
          nullable = true)
      @JsonProperty(value = "type")
      @Null
      StoreType type,

      @Schema(description = "The password of the private key in the local key store file.",
          nullable = true)
      @JsonProperty(value = "key_password")
      @Size(max = KEY_PASSWORD_MAX_LEN)
      @Null
      Password keyPassword
  ) {

    public void validate(
        List<Failure.Error> errors,
        String path,
        String what
    ) {
      if (type == StoreType.UNKNOWN) {
        var values = StoreType.allowedValues();
        errors.add(
            Error.create()
                .withDetail("%s keystore type if provided must be one of: %s", what, values)
                .withSource("%s.type", path)
        );
      }
      if (this.path == null || this.path.isBlank()) {
        errors.add(
            Error.create()
                .withDetail("%s keystore path is required and may not be blank", what)
                .withSource("%s.path", path)
        );
      } else if (this.path.length() > KEYSTORE_PATH_MAX_LEN) {
        errors.add(
            Error.create()
                .withDetail(
                    "%s keystore path may not be longer than %d characters",
                    what,
                    KEYSTORE_PATH_MAX_LEN
                )
                .withSource("%s.path", path)
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
                .withSource("%s.password", path)
        );
      }
    }
  }

  private static final int TRUSTSTORE_PATH_MAX_LEN = 256;
  private static final int TRUSTSTORE_PASSWORD_MAX_LEN = 256;
  private static final int KEYSTORE_PATH_MAX_LEN = 256;
  private static final int KEYSTORE_PASSWORD_MAX_LEN = 256;
  private static final int KEY_PASSWORD_MAX_LEN = 256;
  private static final String DEFAULT_STORE_TYPE = "JKS";

  private static final String DEFAULT_VERIFY_HOSTNAME_VALUE = "true";
  private static final Boolean DEFAULT_VERIFY_HOSTNAME = Boolean.valueOf(
      DEFAULT_VERIFY_HOSTNAME_VALUE
  );

  private static final String DEFAULT_ENABLED_VALUE = "true";
  private static final Boolean DEFAULT_ENABLED = Boolean.valueOf(
      DEFAULT_ENABLED_VALUE
  );

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
     *
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

  /**
   * Default SSL configuration.
   */
  public TLSConfig() {
    this(DEFAULT_VERIFY_HOSTNAME, DEFAULT_ENABLED, null, null);
  }

  public TLSConfig(String truststorePath, Password truststorePassword) {
    this(
        DEFAULT_VERIFY_HOSTNAME,
        DEFAULT_ENABLED,
        new TrustStore(truststorePath, truststorePassword, null),
        null
    );
  }

  public TLSConfig(
      String truststorePath,
      Password truststorePassword,
      String keystorePath,
      Password keystorePassword,
      Password keyPassword
  ) {
    this(
        DEFAULT_VERIFY_HOSTNAME,
        DEFAULT_ENABLED,
        new TrustStore(truststorePath, truststorePassword, null),
        new KeyStore(keystorePath, keystorePassword, null, keyPassword)
    );
  }

  public Optional<Map<String, String>> getProperties(boolean redact) {
    var config = new LinkedHashMap<String, String>();
    if (verifyHostname != null && !verifyHostname) {
      config.put("ssl.endpoint.identification.algorithm", "");
    }

    if (truststore != null) {
      config.put("ssl.truststore.location", truststore.path);
      if (truststore.type != null && truststore.type != StoreType.UNKNOWN) {
        config.put("ssl.truststore.type", truststore.type.name());
      }
      if (truststore.password != null) {
        config.put("ssl.truststore.password", truststore.password.asString(redact));
      }
    }

    if (keystore != null) {
      config.put("ssl.keystore.location", keystore.path);
      if (keystore.type != null && keystore.type != StoreType.UNKNOWN) {
        config.put("ssl.keystore.type", keystore.type.name());
      }
      if (keystore.password != null) {
        config.put("ssl.keystore.password", keystore.password.asString(redact));
      }
      if (keystore.keyPassword != null) {
        config.put("ssl.key.password", keystore.keyPassword.asString(redact));
      }
    }

    return Optional.of(config);
  }

  public void validate(
      List<Failure.Error> errors,
      String path,
      String what
  ) {
    if (enabled != null && !enabled) {
      if (truststore != null) {
        errors.add(
            Error.create()
                .withDetail("%s truststore is not allowed when SSL is disabled", what)
                .withSource("%s.truststore", path)
        );
      }
      if (keystore != null) {
        errors.add(
            Error.create()
                .withDetail("%s keystore is not allowed when SSL is disabled", what)
                .withSource("%s.keystore", path)
        );
      }
    } else {
      if (truststore != null) {
        truststore.validate(errors, "%s.truststore".formatted(path), what);
      }

      if (keystore != null) {
        keystore.validate(errors, "%s.keystore".formatted(path), what);
      }
    }
  }
}
