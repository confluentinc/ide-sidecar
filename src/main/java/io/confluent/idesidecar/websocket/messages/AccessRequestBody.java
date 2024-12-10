package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class AccessRequestBody extends MessageBody {

  public final String accessToken;

  @JsonCreator
  public AccessRequestBody(@JsonProperty("access_token") String accessToken) {
    this.accessToken = accessToken;
  }

}
