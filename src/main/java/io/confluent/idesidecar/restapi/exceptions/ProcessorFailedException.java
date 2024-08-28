package io.confluent.idesidecar.restapi.exceptions;

/**
 * Exception thrown when the Reactive Routes processing chain failed to process a request.
 */
public class ProcessorFailedException extends RuntimeException {

  private final Failure failure;

  public ProcessorFailedException(Failure failure) {
    super(failure.title());
    this.failure = failure;
  }

  public Failure getFailure() {
    return failure;
  }
}
