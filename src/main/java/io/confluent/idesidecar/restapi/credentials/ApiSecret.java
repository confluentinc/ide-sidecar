package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * A user-provided API secret, which must be masked when printed, logged, or
 * returned in an API response.
 */
@JsonDeserialize(using = ApiSecret.Deserializer.class)
@Schema(
    description = "A user-provided API secret that is always masked in responses",
    type = SchemaType.STRING
)
@RegisterForReflection
public class ApiSecret extends Redactable {

  public ApiSecret(char[] raw) {
    super(raw);
  }

  /**
   * The deserializer that parses as a character array rather than as a string,
   * preventing the secret from being stored in memory as a string.
   */
  public static class Deserializer extends BaseDeserializer<ApiSecret> {

    @Override
    protected ApiSecret create(char[] value) {
      return new ApiSecret(value);
    }
  }
}
