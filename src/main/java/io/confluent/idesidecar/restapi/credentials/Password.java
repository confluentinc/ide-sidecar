package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * A user-provided credential or secret, which must be masked when printed, logged, or
 * returned in an API response.
 */
@JsonDeserialize(using = Password.Deserializer.class)
@Schema(
    description = "A user-provided password that is always masked in responses",
    type = SchemaType.STRING
)
@RegisterForReflection
public class Password extends Redactable {

  public Password(char[] raw) {
    super(raw);
  }

  /**
   * The deserializer that parses as a character array rather than as a string,
   * preventing the secret from being stored in memory as a string.
   */
  public static class Deserializer extends BaseDeserializer<Password> {

    @Override
    protected Password create(char[] value) {
      return new Password(value);
    }
  }
}
