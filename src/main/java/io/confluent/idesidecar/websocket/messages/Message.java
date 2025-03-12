package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;
import javax.validation.constraints.NotNull;

/**
 * A message either sent or received through websockets to/from IDE workspaces. Two primary parts,
 * headers and body. The body's structure will vary according to the message messageType.
 *
 * @see MessageHeaders
 * @see MessageBody
 */
@RegisterForReflection
public record Message(
    @NotNull @JsonProperty("headers") MessageHeaders headers,
    @NotNull @JsonProperty("body") MessageBody body
) {

  // Convenience getters for interesting bits from the headers.
  @NotNull
  @JsonIgnore
  public String id() {
    return headers.id();
  }

  @NotNull
  @JsonIgnore
  public MessageType messageType() {
    return headers.messageType();
  }
}
