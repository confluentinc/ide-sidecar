package io.confluent.idesidecar.restapi.credentials;

import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.vertx.core.MultiMap;
import jakarta.validation.constraints.NotNull;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "API key and secret authentication credentials")
@RegisterForReflection
public record ApiKeyAndSecret(

    @Schema(
        description = "The API key to use when connecting to the external service.",
        maxLength = KEY_MAX_LEN,
        minLength = 1
    )
    @JsonProperty(value="api_key")
    @NotNull
    String key,

    @Schema(
        description = "The API secret to use when connecting to the external service.",
        maxLength = ApiSecret.MAX_LENGTH,
        minLength = 1
    )
    @JsonProperty(value="api_secret")
    @NotNull
    ApiSecret secret
) implements Credentials {

  private static final int KEY_MAX_LEN = 64;

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
    tlsConfig.getProperties(options.redact()).ifPresent(config::putAll);
    if (tlsConfig.enabled()) {
      config.put("security.protocol", "SASL_SSL");
    } else {
      config.put("security.protocol", "SASL_PLAINTEXT");
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
    options
        .tlsConfig()
        .getProperties(options.redact())
        .ifPresent(config::putAll);
    config.put(
        "basic.auth.user.info",
        "%s:%s".formatted(key, secret.asString(options.redact()))
    );
    return Optional.of(config);
  }

  /**
   * Create the header(s) for an HTTP client connection. This implementation adds an
   * {@code Authorization} HTTP header of the form:
   * <pre>
   *   Authorization: Basic &lt;credentials>
   * </pre>
   * where {@code &lt;credentials>} is the Base64 encoding of ID (or username) and password
   * joined by a single colon <code>:</code>. See
   * <a href="https://en.wikipedia.org/wiki/Basic_access_authentication">Basic authentication</a>
   * for details.
   *
   * @return the authentication-related HTTP client headers, or empty if these credentials
   *         cannot be used with HTTP client properties
   */
  @Override
  public Optional<MultiMap> httpClientHeaders() {
    var headers = MultiMap.caseInsensitiveMultiMap();
    if (key != null || secret != null) {
      // base64 encode the username and password
      var value = "%s:%s".formatted(key, secret.asString());
      value = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
      headers.add(AUTHORIZATION, "Basic %s".formatted(value));
    }
    return Optional.of(headers);
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
