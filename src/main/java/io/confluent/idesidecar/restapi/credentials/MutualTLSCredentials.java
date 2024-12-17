package io.confluent.idesidecar.restapi.credentials;

import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The mutual TLS credentials object.
 * The {@link #derived} field only serves as the discriminator field
 * to identify the type of {@link Credentials}.
 */
@Schema(description = "Basic authentication credentials")
@RegisterForReflection
public record MutualTLSCredentials(
    @Schema(
        description = "Whether the credentials are derived from a parent object.",
        defaultValue = "true",
        required = true
    )
    Boolean derived
) implements Credentials {

  @Override
  public Type type() {
    return Type.MUTUAL_TLS;
  }

  @Override
  public void validate(List<Failure.Error> errors, String path, String what) {
    // Disallow non-derived credentials
    if (derived == null || !derived) {
      errors.add(
          Failure.Error.create()
              .withDetail(
                  "Non-derived mTLS credentials are not supported. Set 'derived' to true.")
              .withSource("derived")
      );
    }
  }
}
