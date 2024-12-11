package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * A subclass of MessageHeaders that includes a response_to_id field, which is used to link a
 * response message to the message that it is responding to.
 *
 * Used only for outbound, directed messages (Sidecar -> single workspace).
 */
@RegisterForReflection
public class ResponseMessageHeaders extends MessageHeaders {

  @JsonProperty("response_to_id")
  String responseToId;

  /** Constructor for outbound messages. Response messages always originate from the sidecar, and are always directed to a workspace. */
  public ResponseMessageHeaders(String type, String responseToId) {
    super(type, Audience.workspace, "sidecar");
    this.responseToId = responseToId;
  }
}
