package io.confluent.idesidecar.websocket.messages;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.deserializeAndSerialize;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResourceAsObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class MessageSerializationTest {

  /**
   * Test deserializing sample messages from resource files.
   * <p>
   * Proves that the messages deserialize properly into the expected types and body classes.
   * Also verifies that all MessageType enum members are covered by the test cases to
   * force developers to add test cases for new message types.
   */
  @Test
  public void testSerdeResourceMessageFiles() throws IOException {
    // Define the test data as an array of Object arrays
    Object[][] pathAndBodyTypeAndClassTuples = new Object[][]{
        {"websocket-messages/workspace-hello.json", MessageType.WORKSPACE_HELLO, HelloBody.class},
        {"websocket-messages/random-extension-message.json", MessageType.UNKNOWN,
            DynamicMessageBody.class},
        {"websocket-messages/workspaces-changed.json", MessageType.WORKSPACE_COUNT_CHANGED,
            WorkspacesChangedBody.class},
        {"websocket-messages/protocol-error.json", MessageType.PROTOCOL_ERROR,
            ProtocolErrorBody.class}
    };

    // Set to keep track of which MessageType enums have been tested
    Set<MessageType> testedMessageTypes = new HashSet<>();

    for (Object[] tuple : pathAndBodyTypeAndClassTuples) {
      String resourceFilePath = (String) tuple[0];
      MessageType expectedMessageType = (MessageType) tuple[1];
      Class<? extends MessageBody> expectedBodyType = (Class<? extends MessageBody>) tuple[2];

      deserializeAndSerialize(resourceFilePath, Message.class);
      Message message = loadResourceAsObject(resourceFilePath, Message.class);
      Assertions.assertEquals(expectedMessageType, message.messageType());
      Assertions.assertEquals(expectedBodyType, message.body().getClass());
      // while here, prove that the MessageType -> bodyClass mapping is correct.
      Assertions.assertEquals(expectedMessageType.bodyClass(), message.body().getClass());

      testedMessageTypes.add(expectedMessageType);
    }

    // Prove that all MessageType enum members are covered by the test cases
    for (MessageType messageType : MessageType.values()) {
      Assertions.assertTrue(testedMessageTypes.contains(messageType),
          "No test case for MessageType: " + messageType);
    }
  }
}
