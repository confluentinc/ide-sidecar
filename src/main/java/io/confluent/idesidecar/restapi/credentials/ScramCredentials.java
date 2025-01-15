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

@Schema(description = "OAuth 2.0 authentication credentials")
@RecordBuilder
public record ScramCredentials(

    @Schema(description = "Hash algorithm")
    @JsonProperty(value = "hash_algorithm")
    @NotNull
    String hashAlgorithm,
    // ^ should be enum
    @NotNull
    String username,
    @NotNull
    Password password
) implements Credentials {

  @Override
  public Type type() {
    return Type.SCRAM;
  }

//TODO add enum
  private static final String SCRAM_LOGIN_MODULE_CLASS =
      "org.apache.kafka.common.security.scram.ScramLoginModule";


  @Override
  public Optional<Map<String, String>> kafkaClientProperties(
      KafkaConnectionOptions options
  ) {
    var jaasConfig = "%s required username=\"%s\" password=\"%s\"".formatted(
        SCRAM_LOGIN_MODULE_CLASS,
        username,
        password
    );

    // Terminate the JAAS configuration with a semicolon
    jaasConfig += ";";

    var config = new LinkedHashMap<String, String>();
    config.put("sasl.jaas.config", jaasConfig);
    if (hashAlgorithm != null) {
      config.put("sasl.scram.client.hash.algorithm", hashAlgorithm);
    }
    return Optional.of(config);
  }

  @Override
  public void validate(
      List<Failure.Error> errors,
      String path,
      String what
  ) {
    if (hashAlgorithm == null || hashAlgorithm.isBlank()) {
      errors.add(
          Error.create()
              .withDetail("%s Hash algorithm is required and may not be blank", what)
              .withSource("%s.hash_algorithm", path)
      );
    }
    password.validate(errors, path, what);

    if (username == null || username.isBlank()) {
      errors.add(
          Error.create()
              .withDetail("%s Username is required and may not be blank", what)
              .withSource("%s.username", path)
      );
    }
  }
}

