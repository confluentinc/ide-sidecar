package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({
    "self",
    "resource_name",
    "sign_in_uri"
})
public class ConnectionMetadata extends ObjectMetadata {

  @JsonInclude(Include.NON_NULL)
  @JsonProperty(value = "sign_in_uri")
  protected final String signInUri;

  public ConnectionMetadata(String signInUri, String resourcePath, String resourceId) {
    super(resourcePath, resourceId);
    this.signInUri = signInUri;
  }
}
