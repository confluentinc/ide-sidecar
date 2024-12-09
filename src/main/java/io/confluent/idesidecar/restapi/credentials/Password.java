package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * A user-provided credential or secret, which must be masked when printed, logged, or returned in
 * an API response.
 */
@JsonDeserialize(using = Password.Deserializer.class)
@Schema(
    description = "A user-provided password that is always masked in responses",
    type = SchemaType.STRING,
    maxLength = ApiSecret.MAX_LENGTH,
    minLength = 1
)
@RegisterForReflection
public class Password extends Redactable {

  public static final int MAX_LENGTH = 64;

  public Password(char[] raw) {
    super(raw);
  }

  /**
   * The deserializer that parses as a character array rather than as a string, preventing the
   * secret from being stored in memory as a string.
   */
  public static class Deserializer extends BaseDeserializer<Password> {

    @Override
    protected Password create(char[] value) {
      return new Password(value);
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
              .withDetail("%s password may not be longer than %d characters", what, MAX_LENGTH)
              .withSource("%s.password", path)
      );
    }
  }
}
