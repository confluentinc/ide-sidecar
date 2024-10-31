package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.eclipse.microprofile.config.ConfigProvider;

public abstract class BaseList<T> {

  protected static final String API_VERSION = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.api.groupWithVersion", String.class);

  protected final CollectionMetadata metadata;
  protected final List<T> data;

  protected BaseList(List<T> data, CollectionMetadata metadata) {
    this.data = List.copyOf(data); // defensive copy
    this.metadata = metadata;
  }

  @JsonProperty(value = "api_version", required = true)
  public String apiVersion() {
    return API_VERSION;
  }

  @JsonProperty(value = "kind", required = true)
  public String kind() {
    return this.getClass().getSimpleName();
  }

  @JsonProperty(value = "metadata", required = true)
  public CollectionMetadata metadata() {
    return metadata;
  }

  @JsonProperty(value = "data", required = true)
  public List<T> data() {
    return data;
  }
}
