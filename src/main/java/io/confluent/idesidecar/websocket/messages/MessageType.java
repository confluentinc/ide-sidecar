package io.confluent.idesidecar.websocket.messages;


import io.quarkus.runtime.annotations.RegisterForReflection;

/**
  * Pseudo enum holding websocket message types of concern to sidecar.
  * Other message types may exist for use for IDE workspace <--> workspace communication.
 **/
@RegisterForReflection
public class MessageType {
  public static final String WORKSPACE_COUNT_CHANGED = "WORKSPACE_COUNT_CHANGED";
}
