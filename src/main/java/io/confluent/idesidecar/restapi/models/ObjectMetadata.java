package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Objects;
import org.eclipse.microprofile.config.ConfigProvider;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "self",
    "resource_name"
})
public class ObjectMetadata {

  static final String API_HOST = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.api.host", String.class);

  protected static String selfFromResourcePath(String resourcePath, String id) {
    return resourcePath == null ? null : String.format("%s%s/%s", API_HOST, resourcePath, id);
  }

  protected final String resourceName;
  protected final String self;

  @JsonCreator
  public ObjectMetadata(
      @JsonProperty(value = "self") String self,
      @JsonProperty(value = "resource_name") String resourceName
  ) {
    this.resourceName = resourceName;
    this.self = self;
  }

  @JsonProperty(value = "resource_name")
  public String resourceName() {
    return resourceName;
  }

  @JsonProperty(value = "self")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String self() {
    return self;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ObjectMetadata that = (ObjectMetadata) o;
    return Objects.equals(resourceName, that.resourceName)
           && Objects.equals(self, that.self);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceName, self);
  }

  @Override
  public String toString() {
    return "ObjectMetadata{" + "resourceName='" + resourceName + ", self=" + self + '}';
  }
}
