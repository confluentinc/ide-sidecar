package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.quarkus.runtime.annotations.RegisterForReflection;


/**
 * A message either sent or received through websockets to/from IDE workspaces.
 * Two primary parts, headers and body. The body's structure will vary according to the message type
 * and / or the message's audience. Audience.workspaces messages will have bodies that are
 * treated opaquely by sidecar, while others will have definite known structures.
 *
 * @see MessageHeaders
 * @see MessageBody
 * @see MessageDeserializer
 */
@RegisterForReflection
@JsonDeserialize(using = MessageDeserializer.class)
public class Message {
  @JsonProperty("headers")
  private final MessageHeaders headers;

  @JsonProperty("body")
  private final MessageBody body;

  @JsonCreator
  public Message (
      @JsonProperty("headers") MessageHeaders headers,
      @JsonProperty("body") MessageBody body)
  {
    this.headers = headers;
    this.body = body;
  }

  @JsonGetter("headers")
  public MessageHeaders getHeaders() {
    return headers;
  }

  @JsonGetter("body")
  public MessageBody getBody() {
    return body;
  }

  // Convenience getters for interesting bits from the headers.
  @JsonIgnore
  public String getId() {
    return headers.id;
  }

  @JsonIgnore
  public String getType() {
    return headers.type;
  }
}
