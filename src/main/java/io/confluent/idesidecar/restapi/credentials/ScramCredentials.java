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

import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Scram authentication credentials")
@RecordBuilder
public record ScramCredentials(
    @Schema(description = "Hash algorithm")
    @JsonProperty(value = "hash_algorithm")
    @NotNull
    HashAlgorithm hashAlgorithm,
    @NotNull
    String name,
    @NotNull
    Password password
) implements Credentials {

  @Override
  public Type type() {
    return Type.SCRAM;
  }

  private static final String SCRAM_LOGIN_MODULE_CLASS =
      "org.apache.kafka.common.security.scram.ScramLoginModule";


  @Override
  public Optional<Map<String, String>> kafkaClientProperties(
      KafkaConnectionOptions options
  ) {
    var jaasConfig = "%s required name=\"%s\" password=\"%s\"".formatted(
        SCRAM_LOGIN_MODULE_CLASS,
        name,
        password
    );

    // Terminate the JAAS configuration with a semicolon
    jaasConfig += ";";

    var config = new LinkedHashMap<String, String>();
    config.put("sasl.jaas.config", jaasConfig);
    config.put("sasl.scram.client.hash.algorithm", hashAlgorithm.getValue());
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
              .withDetail("%s Hash algorithm is required and may not be blank", what)
              .withSource("%s.hash_algorithm", path)
      );
    }
    password.validate(errors, path, what);

    if (name == null || name.isBlank()) {
      errors.add(
          Error.create()
              .withDetail("%s Username is required and may not be blank", what)
              .withSource("%s.name", path)
      );
    }
  }
}

