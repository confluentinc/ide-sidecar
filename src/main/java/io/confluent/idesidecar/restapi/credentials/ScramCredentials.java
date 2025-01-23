package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.soabase.recordbuilder.core.RecordBuilder;
import jakarta.validation.constraints.NotNull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Scram authentication credentials")
@RecordBuilder
public record ScramCredentials(
    @Schema(description = "Hash algorithm")
    @JsonProperty(value = "hash_algorithm")
    @NotNull
    HashAlgorithm hashAlgorithm,

    @Schema(
        description = "The username to use when connecting to the external service.",
        maxLength = SCRAM_USERNAME_MAX_LEN,
        minLength = 1
    )
    @JsonProperty(value="scram_username")
    @NotNull
    String username,

    @Schema(
        description = "The password to use when connecting to the external service.",
        maxLength = SCRAM_PASSWORD_MAX_LEN,
        minLength = 1
    )
    @JsonProperty(value="scram_password")
    @NotNull
    Password password
) implements Credentials {

  private static final int SCRAM_USERNAME_MAX_LEN = 64;

  private static final int SCRAM_PASSWORD_MAX_LEN= 64;

  public enum HashAlgorithm {
    SCRAM_SHA_256("SCRAM-SHA-256"),
    SCRAM_SHA_512("SCRAM-SHA-512");

    private final String value;

    HashAlgorithm(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  @Override
  public Type type() {
    return Type.SCRAM;
  }

  private static final String SCRAM_LOGIN_MODULE_CLASS = "org.apache.kafka.common.security.scram.ScramLoginModule";

  @Override
  public Optional<Map<String, String>> kafkaClientProperties(
      KafkaConnectionOptions options
  ) {

    var jaasConfig = "%s required username=\"%s\" password=\"%s\";".formatted(
        SCRAM_LOGIN_MODULE_CLASS,
        username,
        password.asString(options.redact())
    );

    var config = new LinkedHashMap<String, String>();
    config.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    config.put(SaslConfigs.SASL_MECHANISM, hashAlgorithm.getValue());
    var tlsConfig = options.tlsConfig();
    if (tlsConfig.enabled()) {
      config.put(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          "SASL_SSL"
      );
    } else {
      config.put(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          "SASL_PLAINTEXT"
      );
    }
    return Optional.of(config);
  }

  @Override
  public void validate(
      List<Failure.Error> errors,
      String path,
      String what
  ) {
    if (hashAlgorithm == null) {
      errors.add(
          Error.create()
              .withDetail(
                  "%s Hash algorithm is required, may not be blank, and must be one of the supported algorithms (SCRAM_SHA_256 or SCRAM_SHA_512)",
                  what
              )
              .withSource(
                  "%s.hash_algorithm",
                  path
              )
      );
    }

    password.validate(errors, path, what);

    if (username == null || username.isBlank()) {
      errors.add(
          Error.create()
              .withDetail(
                  "%s Username is required and may not be blank",
                  what
              )
              .withSource(
                  "%s.scram_username",
                  path
              )
      );
    }
  }
}