package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.soabase.recordbuilder.core.RecordBuilder;
import org.apache.kafka.clients.CommonClientConfigs;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Schema(description = "Kerberos authentication credentials")
@RegisterForReflection
@RecordBuilder
public record KerberosCredentials(
    @Schema(description = "The Kerberos principal to use.",
            required = true)
    @NotNull
    String principal,

    @Schema(description = "The Kerberos keytab file path.", required = true)
    @JsonProperty(value = "keytab_path")
    @NotNull
    String keytabPath,

    @Schema(description = "Service name that matches the primary name of the " +
        "Kafka brokers configured in the Broker JAAS file. Defaults to 'kafka'.",
        defaultValue = "kafka")
    @JsonProperty(value = "service_name")
    String serviceName
) implements Credentials {

  public static final String KERBEROS_CONFIG_FILE_PROPERTY_NAME = "java.security.krb5.conf";

  private static final String KERBEROS_LOGIN_MODULE_CLASS =
      "com.sun.security.auth.module.Krb5LoginModule";

  @Override
  public Type type() {
    return Type.KERBEROS;
  }

  @Override
  public Optional<Map<String, String>> kafkaClientProperties(KafkaConnectionOptions options) {
    var config = new LinkedHashMap<String, String>();
    var tlsConfig = options.tlsConfig();
    if (tlsConfig.enabled()) {
      config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    } else {
      config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    }

    // See: https://docs.confluent.io/platform/current/security/authentication/sasl/gssapi/overview.html#clients
    config.put("sasl.mechanism", "GSSAPI");

    // See: https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html
    config.put(
        "sasl.jaas.config",
        ("%s required " +
            "useKeyTab=true " +
            "doNotPrompt=true " +
            "useTicketCache=false " +
            "keyTab=\"%s\" " +
            "principal=\"%s\";"
        ).formatted(
            KERBEROS_LOGIN_MODULE_CLASS,
            keytabPath,
            principal
        )
    );

    if (serviceName != null) {
      config.put("sasl.kerberos.service.name", serviceName);
    }

    return Optional.of(config);
  }

  @Override
  public Optional<Map<String, String>> schemaRegistryClientProperties(
      SchemaRegistryConnectionOptions options
  ) {
    // Kerberos is not supported as a client authentication mechanism for Schema Registry
    return Optional.empty();
  }

  @Override
  public void validate(
      List<Error> errors,
      String path,
      String what
  ) {
    if (principal == null || principal.isBlank()) {
      errors.add(
          Error.create()
               .withDetail("%s principal is required and may not be blank", what)
               .withSource("%s.principal".formatted(path))
      );
    }
    if (keytabPath == null || keytabPath.isBlank()) {
      errors.add(
          Error.create()
               .withDetail("%s keytab path is required and may not be blank", what)
               .withSource("%s.keytab_path".formatted(path))
      );
    }
  }
}
