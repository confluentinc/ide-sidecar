package io.confluent.idesidecar.scaffolding.exceptions;

/**
 * Exception thrown when the scaffolding request cannot be processed.
 */
public class ProcessException extends Exception {

  public ProcessException(String message) {
    super(message);
  }

  public ProcessException(String message, Throwable cause) {
    super(message, cause);
  }

}
