package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.io.IOException;
import java.util.Map;

/**
  * Enum holding websocket message types of concern to sidecar.
  * Other message types may exist for use for IDE workspace <--> workspace communication.
 **/
@RegisterForReflection
@JsonDeserialize(using = MessageType.MessageTypeDeserializer.class)
public enum MessageType {

  /**
   * Message sent by the workspace to the sidecar when it first connects, combined with
   * a HelloBody body payload. Receipt of this message (containing a valid workspace id
   * in the body) is what will mark this as a good session and cause an increase in sidecar's
   * workspace count.
   */
  WORKSPACE_HELLO,

  /**
   * Message sent by the sidecar to all workspaces when count of connected workspaces
   * (that have sent proper WORKSPACE_HELLO messages) has increased or decreased.
   */
  WORKSPACE_COUNT_CHANGED,

  /**
   * Message sent by sidecar to a workspace when sidecar has noticed a websocket messaging
   * error and is going to disconnect its end of the websocket.
   */
  PROTOCOL_ERROR,

  /**
   * Message sent by sidecar to workspaces when a connection has been created, changed, deleted,
   * or its status has changed.
   */
  CONNECTION_EVENT,

  /**
   * Placeholder for unknown-to-sidecar message types for messages intended to be
   * for extension -> extension messaging via sidecar.
   */
  UNKNOWN;

  /**
   * Static mapping of MessageType to the expected class of the body.
   */
  private static final Map<MessageType, Class<? extends MessageBody>> BODY_CLASS_MAP = Map.of(
      WORKSPACE_HELLO, HelloBody.class,
      WORKSPACE_COUNT_CHANGED, WorkspacesChangedBody.class,
      PROTOCOL_ERROR, ProtocolErrorBody.class,
      CONNECTION_EVENT, ConnectionEventBody.class,
      UNKNOWN, DynamicMessageBody.class
  );

  /**
   * Get the expected class of the body for this MessageType.
   * @return the class of the body
   */
  public Class<? extends MessageBody> bodyClass() {
    return BODY_CLASS_MAP.get(this);
  }

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
