package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.validation.constraints.NotNull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.CommonClientConfigs;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "API key and secret authentication credentials")
@RegisterForReflection
public record ApiKeyAndSecret(

    @Schema(
        description = "The API key to use when connecting to the external service.",
        maxLength = KEY_MAX_LEN,
        minLength = 1
    )
    @JsonProperty(value = "api_key")
    @NotNull
    String key,

    @Schema(
        description = "The API secret to use when connecting to the external service.",
        maxLength = ApiSecret.MAX_LENGTH,
        minLength = 1
    )
    @JsonProperty(value = "api_secret")
    @NotNull
    ApiSecret secret
) implements Credentials {

  private static final int KEY_MAX_LEN = 96;

  private static final String PLAIN_LOGIN_MODULE_CLASS =
      "org.apache.kafka.common.security.plain.PlainLoginModule";

  @Override
  public Type type() {
    return Type.API_KEY_AND_SECRET;
  }

  @Override
  public Optional<Map<String, String>> kafkaClientProperties(
      KafkaConnectionOptions options
  ) {
    var config = new LinkedHashMap<String, String>();
    var tlsConfig = options.tlsConfig();
    if (tlsConfig.enabled()) {
      config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    } else {
      config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    }
    config.put("sasl.mechanism", "PLAIN");
    config.put(
        "sasl.jaas.config",
        "%s required username=\"%s\" password=\"%s\";".formatted(
            PLAIN_LOGIN_MODULE_CLASS,
            key,
            secret.asString(options.redact())
        )
    );
    return Optional.of(config);
  }

  @Override
  public Optional<Map<String, String>> schemaRegistryClientProperties(
      SchemaRegistryConnectionOptions options
  ) {
    var config = new LinkedHashMap<String, String>();
    config.put("basic.auth.credentials.source", "USER_INFO");
    config.put(
        "basic.auth.user.info",
        "%s:%s".formatted(key, secret.asString(options.redact()))
    );
    return Optional.of(config);
  }

  @Override
  public void validate(
      List<Failure.Error> errors,
      String path,
      String what
  ) {
    if (key == null || key.isBlank()) {
      errors.add(
          Error.create()
              .withDetail("%s key is required and may not be blank", what)
              .withSource("%s.key", path)
      );
    } else if (key.length() > KEY_MAX_LEN) {
      errors.add(
          Error.create()
              .withDetail("%s key may not be longer than %d characters", what, KEY_MAX_LEN)
              .withSource("%s.key", path)
      );
    }
    if (secret == null || secret.isEmpty()) {
      errors.add(
          Error.create()
              .withDetail("%s secret is required", what)
              .withSource("%s.secret", path)
      );
    } else {
      secret.validate(errors, path, what);
    }
  }
}
