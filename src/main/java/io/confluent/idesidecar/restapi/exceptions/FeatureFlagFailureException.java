package io.confluent.idesidecar.restapi.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Exception thrown when there is an error fetching feature flags
 */
@RegisterForReflection
public class FeatureFlagFailureException extends RuntimeException {

  private final String code;

  public FeatureFlagFailureException(String message, String code, Throwable t) {
    super(message, t);
    this.code = code;
  }

  public FeatureFlagFailureException(Throwable t) {
    super(t);
    this.code = null;
  }

  public String getCode() {
    return code;
  }
}
