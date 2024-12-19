package io.confluent.idesidecar.websocket.messages;

import static org.junit.Assert.assertNotEquals;

import java.util.HashSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MessageTypeTest {

  /**
   * For each message type, check that the corresponding body class is
   * unique and not null, otherwise someone forgot to add a mapping (or added a duplicate).
   */
  @Test
  public void testBodyClass() {
    var seenClasses = new HashSet<Class<? extends MessageBody>>();
    for (MessageType messageType : MessageType.values()) {
      Assertions.assertNotEquals(null, messageType.bodyClass());
      Assertions.assertNotEquals(true, seenClasses.contains(messageType.bodyClass()));
      seenClasses.add(messageType.bodyClass());
    }
  }
}
