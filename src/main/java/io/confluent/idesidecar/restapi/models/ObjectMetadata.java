package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.eclipse.microprofile.config.ConfigProvider;

@JsonPropertyOrder({
    "self",
    "resource_name"
})
public class ObjectMetadata {

  @JsonIgnore
  protected final String resourcePath;

  @JsonIgnore
  protected final String resourceId;

  public ObjectMetadata(String resourcePath, String resourceId) {
    this.resourcePath = resourcePath;
    this.resourceId = resourceId;
  }

  @JsonProperty
  public String self() {
    var apiHost = ConfigProvider.getConfig().getValue("ide-sidecar.api.host", String.class);

    return String.format("%s%s/%s", apiHost, resourcePath, resourceId);
  }

  @JsonProperty(value = "resource_name")
  public String resourceName() {
    return null;
  }
}
