package io.confluent.idesidecar.restapi.credentials;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * An abstract representation of a sensitive value that should not be printed, logged, or returned
 * in API responses. The value is stored as a character array to prevent accidental exposure, and
 * the {@link #toString()} method always masks the value.
 *
 * <p>The {@link #hashCode()} and {@link #equals(Object)} methods are based upon object identity.
 *
 * @see Password
 */
@RegisterForReflection
public abstract class Redactable {

  public static final String MASKED_VALUE = "*".repeat(8);

  /**
   * A base deserializer for {@link Redactable} values, which creates a new instance from a
   * character array rather than use a string that might be reused.
   *
   * @param <T> the type of {@link Redactable} value
   */
  protected abstract static class BaseDeserializer<T extends Redactable>
      extends JsonDeserializer<T> {

    @Override
    public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      char[] value = p.readValueAs(char[].class); // do NOT deserialize to a string
      return create(value);
    }

    protected abstract T create(char[] value);
  }

  protected abstract static class BaseSerializer<T extends Redactable> extends JsonSerializer<T> {

    @Override
    public void serialize(T value, JsonGenerator gen, SerializerProvider serializers) {
      try {
        var chars = value.asCharArray();
        gen.writeString(chars, 0, chars.length);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final char[] raw;

  protected Redactable(
      char[] raw
  ) {
    Objects.requireNonNull(raw);
    this.raw = Arrays.copyOf(raw, raw.length);
  }

  /**
   * Obtain the redacted value as a character array.
   *
   * @return the redacted literal value
   */
  @JsonIgnore
  public final char[] asCharArray() {
    return Arrays.copyOf(raw, raw.length);
  }

  /**
   * Obtain the credential literal as a string
   *
   * @return the credential as a string
   */
  @JsonIgnore
  public final String asString() {
    return new String(raw);
  }

  /**
   * Obtain the credential literal as either the redacted value or as the raw string
   *
   * @return the credential as a string
   */
  @JsonIgnore
  public final String asString(boolean redact) {
    return redact ? maskedValue() : asString();
  }

  /**
   * Determine whether the redacted value is longer than the specified length.
   *
   * @return whether the redacted value has more characters than the specified value
   */
  @JsonIgnore
  public final boolean longerThan(int length) {
    return raw.length > length;
  }

  /**
   * Determine whether this redacted value is empty.
   * @return true if the redacted value has a length of 0; false otherwise
   */
  @JsonIgnore
  public final boolean isEmpty() {
    return raw.length == 0;
  }

  /**
   * Utility method to obtain the masked value as a string. This is the {@link JsonValue}
   * for OpenAPI generation.
   * @return the masked value
   */
  @JsonValue
  public String maskedValue() {
    return MASKED_VALUE;
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    // We never compare redactable values by their raw contents
    return true;
  }

  @Override
  public final int hashCode() {
    // Do not use the real value
    return maskedValue().hashCode();
  }

  @Override
  public final String toString() {
    return maskedValue();
  }
}