package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import org.eclipse.microprofile.config.ConfigProvider;

@JsonPropertyOrder({
    "api_version",
    "kind",
    "metadata",
    "data"
})
public abstract class BaseList<T> {

  @JsonProperty(value = "api_version", required = true)
  protected String apiVersion = ConfigProvider.getConfig()
      .getValue("ide-sidecar.api.groupWithVersion", String.class);

  @JsonProperty(required = true)
  protected String kind = this.getClass().getSimpleName();

  @JsonProperty(required = true)
  protected CollectionMetadata metadata;

  @JsonProperty(required = true)
  protected List<T> data;
}
