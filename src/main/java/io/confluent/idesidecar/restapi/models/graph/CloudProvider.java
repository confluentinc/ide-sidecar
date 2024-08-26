package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * A cloud service provider.
 */
@RegisterForReflection
public enum CloudProvider {
  AWS,
  AZURE,
  GCP,
  NONE;

  /**
   * Parse the supplied literal value into a {@link CloudProvider} enum value, using a
   * case-insensitive match ignoring leading and trailing whitespace.
   *
   * @param literal the string literal to parse
   * @return the corresponding {@link CloudProvider} enum, or {@link #NONE} if the supplied
   *         literal is null or does not correspond to a particular literal.
   */
  public static CloudProvider of(String literal) {
    if (literal == null) {
      return NONE;
    }
    try {
      return valueOf(literal.toUpperCase().trim());
    } catch (IllegalArgumentException | NullPointerException e) {
      return NONE;
    }
  }
}
