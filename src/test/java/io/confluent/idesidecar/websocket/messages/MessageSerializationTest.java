package io.confluent.idesidecar.websocket.messages;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.deserializeAndSerialize;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResourceAsObject;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
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
    Assertions.assertEquals(expectedMessageType, message.messageType());
    Assertions.assertEquals(expectedBodyType, message.body().getClass());
  }

}
