package io.confluent.idesidecar.restapi.exceptions;

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
