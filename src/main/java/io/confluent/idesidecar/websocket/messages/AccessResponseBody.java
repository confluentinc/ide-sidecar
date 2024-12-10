package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
@JsonSerialize
public class AccessResponseBody extends MessageBody {

  @JsonProperty("authorized")
  public final boolean authorized;

  @JsonProperty("current_workspace_count")
  public final int workspaceCount;

  public AccessResponseBody(boolean authorized, int workspaceCount) {
    this.authorized = authorized;
    this.workspaceCount = workspaceCount;
  }

}
