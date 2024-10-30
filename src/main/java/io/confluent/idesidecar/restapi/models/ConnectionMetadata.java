package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "self",
    "resource_name",
    "sign_in_uri"
})
public class ConnectionMetadata extends ObjectMetadata {

  public static ConnectionMetadata from(
      String signInUri,
      String resourcePath,
      String id
  ) {
    return new ConnectionMetadata(
        selfFromResourcePath(resourcePath, id),
        null,
        signInUri
    );
  }

  protected final String signInUri;

  @JsonCreator
  public ConnectionMetadata(
      @JsonProperty(value = "self") String self,
      @JsonProperty(value = "resource_name") String resourceName,
      @JsonProperty(value = "sign_in_uri") String signInUri
  ) {
    super(self, resourceName);
    this.signInUri = signInUri;
  }

  @JsonInclude(Include.NON_NULL)
  @JsonProperty(value = "sign_in_uri")
  public String signInUri() {
    return signInUri;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConnectionMetadata that = (ConnectionMetadata) o;
    return Objects.equals(resourceName, that.resourceName)
           && Objects.equals(self, that.self)
           && Objects.equals(signInUri, that.signInUri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceName, self, signInUri);
  }

  @Override
  public String toString() {
    return "ConnectionMetadata{" + "resourceName='" + resourceName + ", self=" + self
           + ", signInUri=" + signInUri + '}';
  }
}
