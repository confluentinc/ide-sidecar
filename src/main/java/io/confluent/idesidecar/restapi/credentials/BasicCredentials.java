package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.validation.constraints.NotNull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.CommonClientConfigs;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Basic authentication credentials")
@JsonTypeName("BASIC")
@RegisterForReflection
public record BasicCredentials(
    @Schema(
        description = "The username to use when connecting to the external service.",
        maxLength = USERNAME_MAX_LEN,
        minLength = 1
    )

    @NotNull
    String username,

    @Schema(
        description = "The password to use when connecting to the external service.",
        maxLength = Password.MAX_LENGTH,
        minLength = 1
    )
    @NotNull
    Password password
) implements Credentials {

  private static final int USERNAME_MAX_LEN = 64;

  private static final String PLAIN_LOGIN_MODULE_CLASS =
      "org.apache.kafka.common.security.plain.PlainLoginModule";

  @Override
  public Type type() {
    return Type.BASIC;
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
            username,
            password.asString(options.redact())
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
        "%s:%s".formatted(username, password.asString(options.redact()))
    );
    return Optional.of(config);
  }

  @Override
  public void validate(
      List<Error> errors,
      String path,
      String what
  ) {
    if (username == null || username.isBlank()) {
      errors.add(
          Error.create()
               .withDetail("%s username is required and may not be blank", what)
               .withSource("%s.username".formatted(path))
      );
    } else if (username.length() > USERNAME_MAX_LEN) {
      errors.add(
          Error.create()
               .withDetail("%s username may not be longer than %d characters", what, USERNAME_MAX_LEN)
               .withSource("%s.username", path)
      );
    }
    if (password == null || password.isEmpty()) {
      errors.add(
          Error.create()
               .withDetail("%s password is required", what)
               .withSource("%s.password", path)
      );
    } else {
      password.validate(errors, path, what);
    }
  }
}
