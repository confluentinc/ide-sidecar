package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.quarkus.runtime.annotations.RegisterForReflection;
import javax.validation.constraints.NotNull;

/**
 * A message either sent or received through websockets to/from IDE workspaces.
 * Two primary parts, headers and body. The body's structure will vary according to the message messageType.
 *
 * @see MessageHeaders
 * @see MessageBody
 * @see MessageDeserializer
 */
@RegisterForReflection
@JsonDeserialize(using = MessageDeserializer.class)
public record Message(
    @JsonProperty("headers") MessageHeaders headers,
    @NotNull @JsonProperty("body") MessageBody body
) {

  // Convenience getters for interesting bits from the headers.
  @JsonIgnore
  public String id() {
    return headers.id();
  }

  @JsonIgnore
  public MessageType messageType() {
    return headers.messageType();
  }
}
