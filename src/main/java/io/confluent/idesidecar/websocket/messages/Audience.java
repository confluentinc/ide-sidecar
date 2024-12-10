package io.confluent.idesidecar.websocket.messages;

public enum Audience {
  workspace("workspace"),
  workspaces("workspaces"),
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
