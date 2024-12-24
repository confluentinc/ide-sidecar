package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Null;
import jakarta.validation.constraints.Size;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.CommonClientConfigs;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "OAuth 2.0 authentication credentials")
public record OAuthCredentials(

  @Schema(description = "The URL of the OAuth 2.0 identity provider's token endpoint.")
  @JsonProperty(value = "tokens_url")
  @Size(max = TOKENS_URL_MAX_LEN)
  @NotNull
  String tokensUrl,

  @Schema(description = "The public identifier for the application as registered with the "
                        + "OAuth 2.0 identity provider.")
  @JsonProperty(value = "client_id")
  @Size(min = 1, max = CLIENT_ID_MAX_LEN)
  @NotNull
  String clientId,

  @Schema(description = "The client secret known only to the application and the "
                        + "OAuth 2.0 identity provider.")
  @JsonProperty(value = "client_secret")
  @Size(max = CLIENT_SECRET_MAX_LEN)
  @Null
  Password clientSecret,

  @Schema(description = "The scope to use. The scope is optional and required only when your "
                        + "identity provider doesn't have a default scope or your groups claim is "
                        + "linked to a scope path to use when connecting to the external service.")
  @JsonProperty(value = "scope")
  @Size(max = SCOPE_MAX_LEN)
  @Null
  String scope,

  @Schema(description = "The timeout in milliseconds when connecting to your identity provider.")
  @JsonProperty(value = "connect_timeout_millis")
  @Min(0)
  @Null
  Integer connectTimeoutMillis,

  @Schema(description = "Additional property that can be added in the request header to identify "
                        + "the principal ID for authorization. For example, this may be"
                        + "a Confluent Cloud identity pool.")
  @Null
  String identityPool
) implements Credentials {

  private static final int TOKENS_URL_MAX_LEN = 256;
  private static final int CLIENT_ID_MAX_LEN = 128;
  private static final int CLIENT_SECRET_MAX_LEN = 256;
  private static final int SCOPE_MAX_LEN = 256;

  private static final String OAUTHBEARER_LOGIN_MODULE_CLASS =
      "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";
  private static final String OAUTHBEARER_CALLBACK_CLASS =
      "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler";

  public OAuthCredentials(
      String tokensUrl,
      String clientId,
      Password clientSecret,
      String scope) {
    this(tokensUrl, clientId, clientSecret, scope, null, null);
  }

  public OAuthCredentials(
      String tokensUrl,
      String clientId,
      Password clientSecret
  ) {
    this(tokensUrl, clientId, clientSecret, null, null, null);
  }

  @Override
  public Type type() {
    return Type.OAUTH2;
  }

  @Override
  public Optional<Map<String, String>> kafkaClientProperties(
      KafkaConnectionOptions options
  ) {
    var jaasConfig = "%s required clientId=\"%s\"".formatted(
        OAUTHBEARER_LOGIN_MODULE_CLASS,
        clientId
    );
    if (clientSecret != null) {
      jaasConfig += " clientSecret=\"%s\"".formatted(clientSecret.asString(options.redact()));
    }
    if (scope != null) {
      jaasConfig += " scope=\"%s\"".formatted(scope);
    }

    var config = new LinkedHashMap<String, String>();
    config.put("sasl.mechanism", "OAUTHBEARER");
    config.put("sasl.oauthbearer.token.endpoint.url", tokensUrl);
    config.put("sasl.login.callback.handler.class", OAUTHBEARER_CALLBACK_CLASS);
    config.put("sasl.jaas.config", jaasConfig);
    if (connectTimeoutMillis != null) {
      config.put("sasl.oauthbearer.connect.timeout.ms", connectTimeoutMillis.toString());
    }

    var tlsConfig = options.tlsConfig();
    if (tlsConfig.enabled()) {
      config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    } else {
      config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    }

    return Optional.of(config);
  }

  @Override
  public Optional<Map<String, String>> schemaRegistryClientProperties(
      SchemaRegistryConnectionOptions options
  ) {
    var config = new LinkedHashMap<String, String>();
    config.put("bearer.auth.credentials.source", "OAUTHBEARER");
    config.put("bearer.auth.issuer.endpoint.url", tokensUrl);
    config.put("bearer.auth.client.id", clientId);
    config.put("bearer.auth.client.secret", clientSecret.asString(options.redact()));
    if (scope != null) {
      config.put("bearer.auth.scope", scope);
    }
    if (options.logicalClusterId() != null) {
      config.put("bearer.auth.logical.cluster", options.logicalClusterId());
    }
    if (identityPool != null) {
      config.put("bearer.auth.identity.pool.id", identityPool);
    }
    return Optional.of(config);
  }

  @Override
  public void validate(
      List<Failure.Error> errors,
      String path,
      String what
  ) {
    if (tokensUrl == null || tokensUrl.isBlank()) {
      errors.add(
          Error.create()
               .withDetail("%s OAuth tokens URL is required and may not be blank", what)
               .withSource("%s.tokens_url", path)
      );
    } else if (tokensUrl.length() > TOKENS_URL_MAX_LEN) {
      errors.add(
          Error.create()
               .withDetail(
                   "%s OAuth tokens URL must be at most %d characters",
                   what,
                   TOKENS_URL_MAX_LEN
               )
               .withSource("%s.tokens_url", path)
      );
    } else {
      try {
        new URI(tokensUrl).toURL();
      } catch (URISyntaxException | MalformedURLException e) {
        errors.add(
            Error.create()
                 .withDetail("%s OAuth tokens URL is not a valid URL", what)
                 .withSource("%s.tokens_url", path)
        );
      }
    }
    if (clientId == null || clientId.isBlank()) {
      errors.add(
          Error.create()
               .withDetail("%s OAuth client ID is required and may not be blank", what)
               .withSource("%s.client_id", path)
      );
    } else if (clientId.length() > CLIENT_ID_MAX_LEN) {
      errors.add(
          Error.create()
               .withDetail(
                   "%s OAuth client ID may not be longer than %d characters",
                   what,
                   CLIENT_ID_MAX_LEN
               )
               .withSource("%s.client_id", path)
      );
    }
    if (clientSecret == null) {
      errors.add(
          Error.create()
               .withDetail("%s OAuth client secret is required", what)
               .withSource("%s.client_secret", path)
      );
    } else if (clientSecret.longerThan(CLIENT_SECRET_MAX_LEN)) {
      errors.add(
          Error.create()
               .withDetail(
                   "%s OAuth client secret may not be longer than %d characters",
                   what,
                   CLIENT_SECRET_MAX_LEN
               )
               .withSource("%s.client_secret", path)
      );
    }
    if (scope != null && scope.length() > SCOPE_MAX_LEN) {
      errors.add(
          Error.create()
               .withDetail(
                   "%s OAuth scope may not be longer than %d characters",
                   what,
                   SCOPE_MAX_LEN
               )
               .withSource("%s.scope", path)
      );
    }
    if (connectTimeoutMillis != null && connectTimeoutMillis < 0) {
      errors.add(
          Error.create()
               .withDetail("%s connect timeout in milliseconds must be positive", what)
               .withSource("%s.connect_timeout_millis", path)
      );
    }
  }
}
