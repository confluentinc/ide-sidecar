package io.confluent.idesidecar.restapi.exceptions;

import java.util.Map;

/**
 * Exception thrown when the Reactive Routes processing chain failed to process a request.
 */
public class ProcessorFailedException extends RuntimeException {

  private final Failure failure;
  private final Map<String, String> responseHeaders;

  public ProcessorFailedException(Failure failure) {
    this(failure, Map.of());
  }

  /**
   * A failure whose response also carries {@code responseHeaders}, such as an upstream
   * {@code Retry-After}.
   */
  public ProcessorFailedException(Failure failure, Map<String, String> responseHeaders) {
    super(failure.title());
    this.failure = failure;
    this.responseHeaders = Map.copyOf(responseHeaders);
  }

  public Failure getFailure() {
    return failure;
  }

  public Map<String, String> getResponseHeaders() {
    return responseHeaders;
  }
}
