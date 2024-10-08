package io.confluent.idesidecar.restapi.exceptions;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Exception thrown when there is an error fetching feature flags
 */
@RegisterForReflection
public class FeatureFlagFailureException extends RuntimeException {

  private final ParsingFeatureFlagsFailedException error;

  public FeatureFlagFailureException(String message) {
    super(message);
    this.error = null;
  }

  public FeatureFlagFailureException(ParsingFeatureFlagsFailedException error) {
    super(error.message());
    this.error = error;
  }

  public FeatureFlagFailureException(Throwable t) {
    super(t);
    this.error = null;
  }

  public FeatureFlagFailureException(String message, Throwable t) {
    super(message, t);
    this.error = null;
  }

  public ParsingFeatureFlagsFailedException getError() {
    return error;
  }
}
