package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.eclipse.microprofile.config.ConfigProvider;

@JsonPropertyOrder({
    "api_version",
    "kind",
    "id",
    "metadata",
    "spec"
})
public abstract class BaseModel<SpecT> {

  @JsonProperty(value = "api_version", required = true)
  protected String apiVersion = ConfigProvider.getConfig()
      .getValue("ide-sidecar.api.groupWithVersion", String.class);

  @JsonProperty(required = true)
  protected String kind = this.getClass().getSimpleName();

  @JsonProperty(required = true)
  protected String id;

  @JsonProperty(required = true)
  protected ObjectMetadata metadata;

  @JsonProperty(required = true)
  protected SpecT spec;

  public String getId() {
    return this.id;
  }
}
