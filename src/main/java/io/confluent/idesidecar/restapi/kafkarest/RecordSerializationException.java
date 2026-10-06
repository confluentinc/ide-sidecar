package io.confluent.idesidecar.restapi.kafkarest;

import jakarta.ws.rs.BadRequestException;

/**
 * Indicates which part of a record failed local serialization.
 */
public class RecordSerializationException extends BadRequestException {

  private final MessagePart messagePart;

  /**
   * Creates an exception for a failed record part.
   *
   * @param message the error message
   * @param cause the serialization failure
   * @param messagePart the record part that failed
   */
  public RecordSerializationException(
      String message,
      Throwable cause,
      MessagePart messagePart
  ) {
    super(message, cause);
    this.messagePart = messagePart;
  }

  /**
   * Returns the record part that failed serialization.
   *
   * @return the failed record part
   */
  public MessagePart getMessagePart() {
    return messagePart;
  }

  /**
   * A record part that can fail serialization.
   */
  public enum MessagePart {
    KEY("key"),
    VALUE("value"),
    BOTH("both");

    private final String value;

    MessagePart(String value) {
      this.value = value;
    }

    /**
     * Returns the value exposed in error responses.
     *
     * @return the error-response value
     */
    public String getValue() {
      return value;
    }
  }
}
