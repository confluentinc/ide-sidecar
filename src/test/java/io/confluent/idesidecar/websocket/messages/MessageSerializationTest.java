package io.confluent.idesidecar.websocket.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class MessageSerializationTest {

  /** Test deserializing a simple message of an unknown message type, as if came from sidecar. */
  @Test
  public void testDeserializeRandomSidecarMessage() throws JsonProcessingException {

    ObjectMapper mapper = new ObjectMapper();

    String simpleMessage = """
        {
          "headers": {
            "message_type": "random_sidecar_message",
            "originator": "1122"
          },
          "body": {
            "foonly": 3
          }
        }
        """;

    Message m = mapper.readValue(simpleMessage, Message.class);

    MessageHeaders headers = m.headers();
    assertEquals(MessageType.UNKNOWN, headers.messageType());
    assertEquals("1122", headers.originator());

    MessageBody body = m.body();
    assertTrue(body instanceof DynamicMessageBody);

    DynamicMessageBody dmb = (DynamicMessageBody) body;
    assertEquals(3, dmb.getProperties().get("foonly"));
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
