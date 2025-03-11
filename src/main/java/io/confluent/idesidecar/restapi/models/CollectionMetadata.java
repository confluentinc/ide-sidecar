package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.quarkus.runtime.annotations.RegisterForReflection;
import jakarta.validation.constraints.Null;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "self",
    "next",
    "total_size"
})
@RegisterForReflection
public record CollectionMetadata(
    @JsonProperty(value = "self")
    @Null
    String self,

    @JsonProperty(value = "next")
    @Null
    String next,

    @JsonProperty(value = "total_size")
    int totalSize
) {

  private static String selfFromResourcePath(String resourcePath) {
    return resourcePath == null ? null
        : String.format("%s%s", ObjectMetadata.API_HOST, resourcePath);
  }

  public static CollectionMetadata from(int totalSize, String resourcePath) {
    var self = selfFromResourcePath(resourcePath);
    return new CollectionMetadata(self, null, totalSize);
  }

  public CollectionMetadata withTotalSize(int totalSize) {
    return new CollectionMetadata(self, next, totalSize);
  }
}
