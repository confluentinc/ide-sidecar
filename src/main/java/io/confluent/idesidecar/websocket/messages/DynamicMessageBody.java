package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.HashMap;
import java.util.Map;

/**
 * Arbitrary message body, used when deserializing messages with unknown structure, namely for
 * `Audience.workspaces` messages not intended to be interpreted by sidecar.
 * */
@RegisterForReflection
public class DynamicMessageBody extends MessageBody{
  private final Map<String, Object> properties = new HashMap<String, Object>();

  @JsonAnyGetter
  public Map<String, Object> getProperties() {
    return properties;
  }

  @JsonAnySetter
  public void setProperty(String key, Object value) {
    properties.put(key, value);
  }

  public String toString() {
    return properties.toString();
  }
}
