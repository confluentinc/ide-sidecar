package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;

import java.io.IOException;


/**
 * Custom deserializer for Message objects received from websocket messages from IDE workspaces.
 *
 * Must be a custom deserializer because the body of the message is polymorphic based on both the
 * message type and the audience.
 *
 * If the message is intended for workspaces, the body is deserialized as a DynamicMessageBody which
 * allows arbitrary message bodies.
 *
 * Otherwise, the body is deserialized as a subclass of MessageBody based on the message type.
 */
public class MessageDeserializer extends JsonDeserializer<Message> {

  @Override
  public Message deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    ObjectCodec codec = p.getCodec();
    JsonNode rootNode = codec.readTree(p);

    // Deserialize headers
    JsonNode headersNode = rootNode.get("headers");
    MessageHeaders headers = codec.treeToValue(headersNode, MessageHeaders.class);

    // Determine the body class based on the message type
    Class<? extends MessageBody> bodyClass = getBodyClassForHeader(headers);

    // Deserialize body using the determined class
    JsonNode bodyNode = rootNode.get("body");
    MessageBody body = codec.treeToValue(bodyNode, bodyClass);

    return new Message(headers, body);
  }

  /**
   * Determine the proper MessageBody subclass to use based on the audience and type.
   */
  private Class<? extends MessageBody> getBodyClassForHeader(MessageHeaders headers)
      throws IOException {

    // If the message is intended for workspaces, use the DynamicMessageBody class which
    // allows arbitrary message bodies.
    if (headers.audience == Audience.WORKSPACES) {
      return DynamicMessageBody.class;
    }

    // Otherwise map the type to the appropriate MessageBody subclass. As we get more
    // of these, probably defer to a Map.

    // Right now .... we don't have any!

    //if (headers.type.equals(MessageType.ACCESS_REQUEST)) {
    //  return AccessRequestBody.class;
    //}

    throw new IOException("Unknown message type: " + headers.type);
  }
}