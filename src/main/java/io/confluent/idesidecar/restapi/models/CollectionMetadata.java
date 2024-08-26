package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.eclipse.microprofile.config.ConfigProvider;

@JsonPropertyOrder({
    "self",
    "next",
    "total_size"
})
public record CollectionMetadata(
    @JsonProperty(value = "total_size")
    int totalSize,
    @JsonIgnore
    String resourcePath
) {

  @JsonProperty
  public String self() {
    var apiHost = ConfigProvider.getConfig().getValue("ide-sidecar.api.host", String.class);

    return String.format("%s%s", apiHost, resourcePath);
  }

  @JsonProperty
  public String next() {
    return null;
  }
}
