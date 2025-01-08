package io.confluent.idesidecar.websocket.messages;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.deserializeAndSerialize;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResourceAsObject;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import org.junit.Test;

public class MessageSerializationTest {

  /**
   * Test deserializing sample messages from resource files.
   *
   * <p>Proves that the messages deserialize properly into the expected types and body classes.
   * Also verifies that all MessageType enum members are covered by the test cases to
   * force developers to add test cases for new message types.
   */
  @Test
  public void testSerdeResourceMessageFiles() {
    // Define the test data as an array of Object arrays
    record TestInput(
        String resourceFilePath,
        MessageType expectedMessageType,
        Class<? extends MessageBody> expectedBodyType
    ) {
    }

    var inputs = List.of(
        new TestInput(
            "websocket-messages/workspace-hello.json",
            MessageType.WORKSPACE_HELLO,
            HelloBody.class
        ),
        new TestInput(
            "websocket-messages/random-extension-message.json",
            MessageType.UNKNOWN,
            DynamicMessageBody.class
        ),
        new TestInput(
            "websocket-messages/workspaces-changed.json",
            MessageType.WORKSPACE_COUNT_CHANGED,
            WorkspacesChangedBody.class
        ),
        new TestInput(
            "websocket-messages/protocol-error.json",
            MessageType.PROTOCOL_ERROR,
            ProtocolErrorBody.class
        ),
        new TestInput(
            "websocket-messages/connection-valid.json",
            MessageType.CONNECTION_EVENT,
            ConnectionEventBody.class
        ),
        new TestInput(
            // Has some java.time.Instant fields, not serializable by default
            // ObjectMapper instances.
            "websocket-messages/ccloud-connection-connected.json",
            MessageType.CONNECTION_EVENT,
            ConnectionEventBody.class
        )
    );

    // Set to keep track of which MessageType enums have been tested
    var testedMessageTypes = new HashSet<>();
    inputs.forEach(input -> {
      deserializeAndSerialize(input.resourceFilePath(), Message.class);
      Message message = loadResourceAsObject(input.resourceFilePath(), Message.class);
      assertNotNull(message);
      assertEquals(
          input.expectedMessageType(),
          message.messageType())
      ;
      assertEquals(
          input.expectedBodyType(),
          message.body().getClass()
      );
      // while here, prove that the MessageType -> bodyClass mapping is correct.
      assertEquals(
          input.expectedMessageType().bodyClass(),
          message.body().getClass()
      );

      testedMessageTypes.add(input.expectedMessageType());
    });

    // Prove that all MessageType enum members are covered by the test cases
    for (MessageType messageType : MessageType.values()) {
      assertTrue(
          testedMessageTypes.contains(messageType),
          "No test case for MessageType: " + messageType
      );
    }
  }
}