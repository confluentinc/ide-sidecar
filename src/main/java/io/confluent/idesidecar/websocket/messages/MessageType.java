package io.confluent.idesidecar.websocket.messages;


/**
  * Pseudo enum holding websocket message types of concern to sidecar.
  * Other message types may exist for use for IDE workspace <--> workspace communication.
 **/
public class MessageType {
  public static final String ACCESS_REQUEST = "ACCESS_REQUEST";
  public static final String ACCESS_RESPONSE = "ACCESS_RESPONSE";

  public static final String WORKSPACE_COUNT_CHANGED = "WORKSPACE_COUNT_CHANGED";
}
