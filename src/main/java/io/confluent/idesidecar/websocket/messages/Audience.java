package io.confluent.idesidecar.websocket.messages;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public enum Audience {
  /**
   * Message intended for a single workspace, usually originating from the sidecar as a response
   * to a message from the workspace.
   */
  workspace("workspace"),

  /**
   * Message intended for all workspaces, originating either from the sidecar or a workspace.
   */
  workspaces("workspaces"),

  /**
   * Message intended for the sidecar, originating from a workspace.
   */
  sidecar("sidecar");

  private final String text;

  Audience(String text) {
    this.text = text;
  }

  @Override
  public String toString() {
    return text;
  }
}
