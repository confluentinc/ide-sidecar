package io.confluent.idesidecar.websocket.messages;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.deserializeAndSerialize;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResourceAsObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class MessageSerializationTest {



  static Stream<Object []> pathAndBodyTypePairs() {
    return Stream.of(
        new Object [] {"websocket-messages/random-extension-message.json", MessageType.UNKNOWN, DynamicMessageBody.class},
        new Object [] {"websocket-messages/workspaces-changed.json", MessageType.WORKSPACE_COUNT_CHANGED, WorkspacesChangedBody.class},
        new Object [] {"websocket-messages/protocol-error.json", MessageType.PROTOCOL_ERROR, ProtocolErrorBody.class}
    );
  }
  /** Test deserializing sample messages from resource files. Prove that the messages deserialize properly into the expected types and body classes. */
  @ParameterizedTest
  @MethodSource("pathAndBodyTypePairs")
  public void testSerdeResourceMessageFiles(String resourceFilePath, MessageType expectedMessageType, Class<MessageBody> expectedBodyType) throws IOException {
    deserializeAndSerialize(resourceFilePath, Message.class);
    Message message = loadResourceAsObject(resourceFilePath, Message.class);
    assertEquals(expectedMessageType, message.messageType());
    assertEquals(expectedBodyType, message.body().getClass());
  }


  @Test
  public void MessageEqualityTests() throws IOException  {
    MessageHeaders headers = new MessageHeaders(MessageType.WORKSPACE_COUNT_CHANGED, "sidecar", "message-id-here");

    String serialized = new ObjectMapper().writeValueAsString(headers);
    MessageHeaders headersDeserialized = new ObjectMapper().readValue(serialized, MessageHeaders.class);

    assertEquals(headers, headersDeserialized);



    WorkspacesChangedBody body = new WorkspacesChangedBody(3);
    serialized = new ObjectMapper().writeValueAsString(body);
    WorkspacesChangedBody bodyDeserialized = new ObjectMapper().readValue(serialized, WorkspacesChangedBody.class);

    assertEquals(body, bodyDeserialized);

    Message m = new Message(headers, body);
    serialized = new ObjectMapper().writeValueAsString(m);
    Message mDeserialized = new ObjectMapper().readValue(serialized, Message.class);

    assertEquals(m, mDeserialized);

  }

  /** Test serializing a known sidecar -> workspaces message. */
  @Test
  public void testSerializationOfKnownBody() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();

    MessageHeaders headers = new MessageHeaders(MessageType.WORKSPACE_COUNT_CHANGED, "sidecar", "message-id-here");
    WorkspacesChangedBody body = new WorkspacesChangedBody(3);

    Message m = new Message(headers, body);

    String serialized = mapper.writeValueAsString(m);

    String expected = """
        {"headers":{"message_type":"WORKSPACE_COUNT_CHANGED","originator":"sidecar","message_id":"message-id-here"},"body":{"current_workspace_count":3}}
        """.strip();

    assertEquals(expected, serialized);
  }
}
