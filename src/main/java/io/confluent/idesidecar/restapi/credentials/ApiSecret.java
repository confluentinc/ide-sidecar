package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * A user-provided API secret, which must be masked when printed, logged, or returned in an API
 * response.
 */
@JsonDeserialize(using = ApiSecret.Deserializer.class)
@Schema(
    description = "A user-provided API secret that is always masked in responses",
    type = SchemaType.STRING,
    maxLength = ApiSecret.MAX_LENGTH,
    minLength = 1
)
@RegisterForReflection
public class ApiSecret extends Redactable {

  public static final int MAX_LENGTH = 64;

  public ApiSecret(char[] raw) {
    super(raw);
  }

  /**
   * The deserializer that parses as a character array rather than as a string, preventing the
   * secret from being stored in memory as a string.
   */
  public static class Deserializer extends BaseDeserializer<ApiSecret> {

    @Override
    protected ApiSecret create(char[] value) {
      return new ApiSecret(value);
    }
  }

  public void validate(
      List<Failure.Error> errors,
      String path,
      String what
  ) {
    if (longerThan(Password.MAX_LENGTH)) {
      errors.add(
          Error.create()
              .withDetail("%s API secret may not be longer than %d characters", what, MAX_LENGTH)
              .withSource("%s.password", path)
      );
    }
  }
}
