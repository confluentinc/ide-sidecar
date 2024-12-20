package io.confluent.idesidecar.websocket.messages;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashSet;
import org.junit.jupiter.api.Test;

public class MessageTypeTest {

  /**
   * For each message type, check that the corresponding body class is
   * unique and not null, otherwise someone forgot to add a mapping (or added a duplicate).
   */
  @Test
  public void testBodyClass() {
    var seenClasses = new HashSet<Class<? extends MessageBody>>();
    for (var messageType : MessageType.values()) {
      assertNotNull(
          messageType.bodyClass(),
          "Expected body class to be non-null for MessageType: " + messageType
      );
      assertFalse(
          seenClasses.contains(messageType.bodyClass()),
          "Expected body class to be unique for MessageType: " + messageType
      );
      seenClasses.add(messageType.bodyClass());
    }
  }
}
