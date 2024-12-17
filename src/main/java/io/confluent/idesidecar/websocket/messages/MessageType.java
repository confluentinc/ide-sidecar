package io.confluent.idesidecar.websocket.messages;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.io.IOException;

/**
  * Enum holding websocket message types of concern to sidecar.
  * Other message types may exist for use for IDE workspace <--> workspace communication.
 **/
@RegisterForReflection
@JsonDeserialize(using = MessageType.MessageTypeDeserializer.class)
public enum MessageType {
  /** Message sent by the sidecar to the workspace when the list of workspaces has changed. */
  WORKSPACE_COUNT_CHANGED,
  /** Message sent by sidecar to a workspace when sidecar has noticed an error and
   * is going to disconnect its end of the websocket. */
  PROTOCOL_ERROR,

  /** Placeholder for unknown-to-sidecar message types for messages intended to be
   * for extension -> extension messaging via sidecar. */
  UNKNOWN;

  /**
   * Custom method to parse the MessageType from a string, using case-insensitive mapping and
   * defaulting to the {@link #UNKNOWN} literal.
   * @param type the string to parse
   * @return the MessageType corresponding to the string, or {@link #UNKNOWN} if not found
   */
  public static MessageType fromString(String type) {
    try {
      return MessageType.valueOf(type.toUpperCase());
    } catch (IllegalArgumentException e) {
      return UNKNOWN;
    }
  }

  /**
   * Custom deserializer that uses {@link #fromString(String)} to parse the MessageType from a JSON
   * in a case-insensitive manner and defaulting to {@link #UNKNOWN}.
   */
  public static class MessageTypeDeserializer extends JsonDeserializer<MessageType> {
    @Override
    public MessageType deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return MessageType.fromString(p.getValueAsString());
    }
  }
}
