package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;

import java.io.IOException;


/**
 * Custom deserializer for {@link Message} objects received from websocket messages from IDE workspaces.
 *
 * Must be a custom deserializer because the body of the message will be polymorphic based on both the
 * message type and future audience.
 *
 * If the message is intended for workspaces, the body is deserialized as a DynamicMessageBody which
 * allows arbitrary message bodies (the only expected use currently).
 *
 * Otherwise, future work if needed, the body is deserialized as an varying implementation of
 * MessageBody based on the message type.
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
  {
    switch (headers.type()) {
      // Workspaces never send this, but is both a good example as well as helps out the test suite.
      case MessageType.WORKSPACE_COUNT_CHANGED:
        return WorkspacesChangedBody.class;
      default:
        return DynamicMessageBody.class;
    }
  }
}