package io.confluent.idesidecar.websocket.messages;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Arbitrary message body, used when deserializing messages with unknown structure, namely for
 * messages not intended to be interpreted by sidecar.
 * */
@RegisterForReflection
public class DynamicMessageBody implements MessageBody {
  private final Map<String, Object> properties = new LinkedHashMap<>(); // retain order

  public DynamicMessageBody() {
  }

  public DynamicMessageBody(Map<String, Object> properties) {
    this.properties.putAll(properties);
  }

  @JsonAnyGetter
  public Map<String, Object> getProperties() {
    return properties;
  }

  @JsonAnySetter
  public void setProperty(String key, Object value) {
    properties.put(key, value);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    DynamicMessageBody that = (DynamicMessageBody) obj;
    return properties.equals(that.properties);
  }

  @Override
  public int hashCode() {
    return properties.hashCode();
  }
}
