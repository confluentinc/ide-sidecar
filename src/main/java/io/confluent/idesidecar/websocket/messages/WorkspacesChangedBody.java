package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Sent by the sidecar to the workspace when the list of workspaces has changed.
 */
@RegisterForReflection
public final class WorkspacesChangedBody extends MessageBody {

  @JsonProperty("current_workspace_count")
  public final int workspaceCount;

  public WorkspacesChangedBody(int workspaceCount)
  {
    this.workspaceCount = workspaceCount;
  }
}
